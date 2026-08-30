use std::collections::BTreeSet;

use cymule_core::{
    ArtifactRecord, ArtifactRef, PlanCandidate, ReconciliationResolution, RunExecutionStatus,
    RunFailure, WorldSettlementStatus,
};
use cymule_profile_protocol::{
    agent as agent_protocol, evolution as evolution_protocol, resource as resource_protocol,
    virtual_work as virtual_protocol,
};
use cymule_runtime::{BoundPluginHost, ExecutionBindingAdmission, ExecutionResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::coordinator::DurableCoordinator;
use crate::executor::{EffectResolutionRuntime, ResumableRuntime};
use crate::{
    ComponentOccurrence, ComponentOccurrenceState, ComponentOutcome, ContinuationStatus,
    DriveOutcome, DurableError, DurableResult, DurableStore, EffectDispatch, ExecutionClaimRequest,
    OperationAttempt, OperationAttemptState, OutboxState, WaitActivationSource, WaitCondition,
    WaitState,
};

/// Frozen provider-neutral M1 control protocol version.
pub const DURABLE_CONTROL_VERSION: &str = "cymule.durable-control/4";
/// Maximum entry count returned by one bounded durable query page.
pub const MAX_DURABLE_QUERY_PAGE_ITEMS: u32 = 256;
/// Maximum canonical bytes returned by one bounded durable query page.
pub const MAX_DURABLE_QUERY_PAGE_BYTES: u64 = 1024 * 1024;
/// Maximum canonical bytes admitted for one fixed-size query summary.
pub const MAX_DURABLE_QUERY_SUMMARY_BYTES: usize = 32 * 1024;
/// Maximum Unicode scalar count of a Run identity retained in a query cursor.
pub const MAX_DURABLE_QUERY_RUN_KEY_SCALARS: usize = 512;
/// Maximum canonical bytes returned by one exact leaf read. The extra page-sized
/// envelope keeps every legal 12 MiB `StateRoot` leaf reachable.
pub const MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES: u64 =
    crate::MAX_STATE_ROOT_LEAF_BYTES as u64 + MAX_DURABLE_QUERY_PAGE_BYTES;
/// Frozen receipt for one provider-independent Effect resolution.
pub const EFFECT_RESOLUTION_RECEIPT_VERSION: &str = "cymule.effect-resolution-receipt/1";
/// Frozen receipt for one semantic Run cancellation.
pub const RUN_CANCELLATION_RECEIPT_VERSION: &str = "cymule.run-cancellation-receipt/1";

/// Closed stateful control and query union for one durable domain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableCommand {
    /// Seal, create, and drive one Run to its next durable boundary.
    StartRun {
        /// Protocol version.
        control_version: String,
        /// Stable Run identity and idempotency key.
        run_id: String,
        /// Candidate sealed by the trusted Rust runtime.
        candidate: PlanCandidate,
        /// Immutable initial input.
        input: Value,
        /// Exact execution authority for the initial driver.
        execution: ExecutionClaimRequest,
    },
    /// Resume one existing ready or running Run.
    ResumeRun {
        /// Protocol version.
        control_version: String,
        /// Existing Run identity.
        run_id: String,
        /// Exact execution authority for the new driver.
        execution: ExecutionClaimRequest,
    },
    /// Explicitly take over one expired persisted Running Run.
    TakeoverRun {
        /// Protocol version.
        control_version: String,
        /// Existing Run identity.
        run_id: String,
        /// Exact current fence observed by the caller.
        expected_fence: u64,
        /// Exact execution authority for the takeover driver.
        execution: ExecutionClaimRequest,
    },
    /// Admit one identified external signal or timer observation.
    ActivateWait {
        /// Protocol version.
        control_version: String,
        /// Stable transport delivery and deduplication identity.
        activation_id: String,
        /// Plan-declared activation source.
        source: WaitActivationSource,
        /// Exact targets selected from the parked index.
        wait_ids: BTreeSet<String>,
        /// Typed result sealed to an Artifact by Rust before admission.
        value: Value,
    },
    /// Explicitly release one prepared effect after its scope committed.
    ReleaseEffect {
        /// Protocol version.
        control_version: String,
        /// Structural effect intent identity.
        intent_id: String,
        /// Exact execution authority for dispatch/reconciliation.
        execution: ExecutionClaimRequest,
    },
    /// Resolve one retained unknown-world Effect under its original binding
    /// and dispatch fence without resuming execution or dispatching again.
    ResolveEffect {
        /// Protocol version.
        control_version: String,
        /// Stable resolution and idempotency identity.
        resolution_id: String,
        /// Owning Run retained by the original intent.
        run_id: String,
        /// Structural effect intent identity.
        intent_id: String,
        /// Exact historical execution-binding Artifact.
        execution_binding: ArtifactRef,
        /// Exact occurrence binding derived from that Artifact.
        occurrence_binding: String,
        /// Original dispatch-claim owner retained after ambiguity.
        claim_owner: String,
        /// Original dispatch-claim fence retained after ambiguity.
        claim_epoch: u64,
        /// Closed terminal world resolution.
        resolution: ReconciliationResolution,
        /// Optional authoritative output value; required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        value: Option<Value>,
    },
    /// Cancel one Run without requiring a live execution provider.
    CancelRun {
        /// Protocol version.
        control_version: String,
        /// Stable cancellation and idempotency identity.
        cancellation_id: String,
        /// Existing Run identity.
        run_id: String,
        /// Provider-neutral semantic reason sealed by Rust.
        reason: Value,
    },
    /// Read one revision-pinned page of the domain's Run index.
    RunIndexPage {
        /// Protocol version.
        control_version: String,
        /// Optional exact revision precondition, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        expected_revision: Option<String>,
        /// Optional continuation from the preceding page, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        cursor: Option<DurablePageCursor>,
        /// Maximum number of summaries to return.
        limit: u32,
        /// Maximum canonical response bytes accepted by the caller.
        max_canonical_bytes: u64,
    },
    /// Read one Run's bounded semantic current projection.
    RunCurrent {
        /// Protocol version.
        control_version: String,
        /// Run to inspect.
        run_id: String,
        /// Optional exact revision precondition, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        expected_revision: Option<String>,
    },
    /// Read one revision-pinned page of a Run's waits.
    RunWaitPage {
        /// Protocol version.
        control_version: String,
        /// Owning Run.
        run_id: String,
        /// Optional exact revision precondition, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        expected_revision: Option<String>,
        /// Optional continuation from the preceding page, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        cursor: Option<DurablePageCursor>,
        /// Maximum number of summaries to return.
        limit: u32,
        /// Maximum canonical response bytes accepted by the caller.
        max_canonical_bytes: u64,
    },
    /// Read one revision-pinned page of a Run's Effects.
    RunEffectPage {
        /// Protocol version.
        control_version: String,
        /// Owning Run.
        run_id: String,
        /// Optional exact revision precondition, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        expected_revision: Option<String>,
        /// Optional continuation from the preceding page, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        cursor: Option<DurablePageCursor>,
        /// Maximum number of summaries to return.
        limit: u32,
        /// Maximum canonical response bytes accepted by the caller.
        max_canonical_bytes: u64,
    },
    /// Read one revision-pinned page of a Run's component occurrences.
    RunOccurrencePage {
        /// Protocol version.
        control_version: String,
        /// Owning Run.
        run_id: String,
        /// Optional exact revision precondition, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        expected_revision: Option<String>,
        /// Optional continuation from the preceding page, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        cursor: Option<DurablePageCursor>,
        /// Maximum number of summaries to return.
        limit: u32,
        /// Maximum canonical response bytes accepted by the caller.
        max_canonical_bytes: u64,
    },
    /// Read one revision-pinned page of a Run's provider Attempts.
    RunAttemptPage {
        /// Protocol version.
        control_version: String,
        /// Owning Run.
        run_id: String,
        /// Optional exact revision precondition, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        expected_revision: Option<String>,
        /// Optional continuation from the preceding page, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        cursor: Option<DurablePageCursor>,
        /// Maximum number of summaries to return.
        limit: u32,
        /// Maximum canonical response bytes accepted by the caller.
        max_canonical_bytes: u64,
    },
    /// Read one complete typed `StateRoot` leaf by exact Run-owned identity.
    RunItem {
        /// Protocol version.
        control_version: String,
        /// Owning Run.
        run_id: String,
        /// Optional exact revision precondition, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        expected_revision: Option<String>,
        /// Exact typed item selector.
        selector: DurableRunItemSelector,
        /// Maximum canonical response bytes accepted by the caller.
        max_canonical_bytes: u64,
    },
}

impl DurableCommand {
    /// Return whether this command observes durable authority without mutation.
    pub const fn is_read_only(&self) -> bool {
        matches!(
            self,
            Self::RunIndexPage { .. }
                | Self::RunCurrent { .. }
                | Self::RunWaitPage { .. }
                | Self::RunEffectPage { .. }
                | Self::RunOccurrencePage { .. }
                | Self::RunAttemptPage { .. }
                | Self::RunItem { .. }
        )
    }

    /// Return whether execution requires a live operation provider.
    pub const fn requires_executor(&self) -> bool {
        matches!(
            self,
            Self::StartRun { .. }
                | Self::ResumeRun { .. }
                | Self::TakeoverRun { .. }
                | Self::ReleaseEffect { .. }
                | Self::ResolveEffect { .. }
        )
    }

    /// Return whether execution ownership must consume a current Clock head.
    pub const fn requires_clock(&self) -> bool {
        matches!(
            self,
            Self::StartRun { .. }
                | Self::ResumeRun { .. }
                | Self::TakeoverRun { .. }
                | Self::ReleaseEffect { .. }
        )
    }

    /// Validate the closed command independently of current durable state.
    ///
    /// # Errors
    /// Returns an error for an unsupported version, invalid semantic identity, Plan,
    /// execution request, settlement command, or query bounds and cursor.
    pub fn verify(&self) -> DurableResult<()> {
        self.verify_version()?;
        if self.is_read_only() {
            self.verify_query()
        } else {
            self.verify_mutation()
        }
    }

    fn verify_version(&self) -> DurableResult<()> {
        let version = match self {
            Self::StartRun {
                control_version, ..
            }
            | Self::ResumeRun {
                control_version, ..
            }
            | Self::TakeoverRun {
                control_version, ..
            }
            | Self::ActivateWait {
                control_version, ..
            }
            | Self::ReleaseEffect {
                control_version, ..
            }
            | Self::ResolveEffect {
                control_version, ..
            }
            | Self::CancelRun {
                control_version, ..
            }
            | Self::RunIndexPage {
                control_version, ..
            }
            | Self::RunCurrent {
                control_version, ..
            }
            | Self::RunWaitPage {
                control_version, ..
            }
            | Self::RunEffectPage {
                control_version, ..
            }
            | Self::RunOccurrencePage {
                control_version, ..
            }
            | Self::RunAttemptPage {
                control_version, ..
            }
            | Self::RunItem {
                control_version, ..
            } => control_version,
        };
        if version != DURABLE_CONTROL_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported durable control version {version}"
            )));
        }
        Ok(())
    }

    fn verify_mutation(&self) -> DurableResult<()> {
        match self {
            Self::StartRun {
                run_id,
                candidate,
                execution,
                ..
            } => {
                validate_identity("Run", run_id)?;
                cymule_core::seal_plan(candidate.clone())?;
                execution.verify()?;
            }
            Self::ResumeRun {
                run_id, execution, ..
            } => {
                validate_identity("Run", run_id)?;
                execution.verify()?;
            }
            Self::TakeoverRun {
                run_id,
                expected_fence,
                execution,
                ..
            } => {
                validate_identity("Run", run_id)?;
                if *expected_fence == 0 || *expected_fence > crate::MAX_EXACT_INTEGER {
                    return Err(DurableError::Validation(
                        "takeover expected fence must use the exact positive cross-language range"
                            .to_owned(),
                    ));
                }
                execution.verify()?;
            }
            Self::ActivateWait {
                activation_id,
                source,
                wait_ids,
                ..
            } => verify_wait_activation_command(activation_id, source, wait_ids)?,
            Self::ReleaseEffect {
                intent_id,
                execution,
                ..
            } => {
                crate::model::validate_sha256_identity("effect intent", intent_id)?;
                execution.verify()?;
            }
            Self::ResolveEffect {
                resolution_id,
                run_id,
                intent_id,
                execution_binding,
                occurrence_binding,
                claim_owner,
                claim_epoch,
                resolution,
                value,
                ..
            } => EffectResolutionCommand {
                resolution_id: resolution_id.clone(),
                run_id: run_id.clone(),
                intent_id: intent_id.clone(),
                execution_binding: execution_binding.clone(),
                occurrence_binding: occurrence_binding.clone(),
                claim_owner: claim_owner.clone(),
                claim_epoch: *claim_epoch,
                resolution: *resolution,
                value: value.clone(),
            }
            .verify()?,
            Self::CancelRun {
                cancellation_id,
                run_id,
                reason,
                ..
            } => CancellationCommand {
                cancellation_id: cancellation_id.clone(),
                run_id: run_id.clone(),
                reason: reason.clone(),
            }
            .verify()?,
            Self::RunIndexPage { .. }
            | Self::RunCurrent { .. }
            | Self::RunWaitPage { .. }
            | Self::RunEffectPage { .. }
            | Self::RunOccurrencePage { .. }
            | Self::RunAttemptPage { .. }
            | Self::RunItem { .. } => {
                return Err(DurableError::RuntimeDefect {
                    code: "query_bypassed_control_dispatch".to_owned(),
                    message: "a verified query bypassed the closed query dispatch".to_owned(),
                });
            }
        }
        Ok(())
    }

    fn verify_query(&self) -> DurableResult<()> {
        match self {
            Self::RunIndexPage { .. }
            | Self::RunWaitPage { .. }
            | Self::RunEffectPage { .. }
            | Self::RunOccurrencePage { .. }
            | Self::RunAttemptPage { .. } => self.verify_paged_query(),
            Self::RunCurrent {
                run_id,
                expected_revision,
                ..
            } => verify_exact_query(run_id, expected_revision.as_deref()),
            Self::RunItem {
                run_id,
                expected_revision,
                selector,
                max_canonical_bytes,
                ..
            } => verify_run_item_query(
                run_id,
                expected_revision.as_deref(),
                selector,
                *max_canonical_bytes,
            ),
            Self::StartRun { .. }
            | Self::ResumeRun { .. }
            | Self::TakeoverRun { .. }
            | Self::ActivateWait { .. }
            | Self::ReleaseEffect { .. }
            | Self::ResolveEffect { .. }
            | Self::CancelRun { .. } => Err(DurableError::Validation(
                "durable mutation reached the read-only query verifier".to_owned(),
            )),
        }
    }
    fn verify_paged_query(&self) -> DurableResult<()> {
        match self {
            Self::RunIndexPage {
                expected_revision,
                cursor,
                limit,
                max_canonical_bytes,
                ..
            } => verify_page_query(
                DurablePageQueryKind::RunIndex,
                None,
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            Self::RunWaitPage {
                run_id,
                expected_revision,
                cursor,
                limit,
                max_canonical_bytes,
                ..
            } => verify_page_query(
                DurablePageQueryKind::RunWaits,
                Some(run_id),
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            Self::RunEffectPage {
                run_id,
                expected_revision,
                cursor,
                limit,
                max_canonical_bytes,
                ..
            } => verify_page_query(
                DurablePageQueryKind::RunEffects,
                Some(run_id),
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            Self::RunOccurrencePage {
                run_id,
                expected_revision,
                cursor,
                limit,
                max_canonical_bytes,
                ..
            } => verify_page_query(
                DurablePageQueryKind::RunOccurrences,
                Some(run_id),
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            Self::RunAttemptPage {
                run_id,
                expected_revision,
                cursor,
                limit,
                max_canonical_bytes,
                ..
            } => verify_page_query(
                DurablePageQueryKind::RunAttempts,
                Some(run_id),
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            Self::RunCurrent { .. }
            | Self::RunItem { .. }
            | Self::StartRun { .. }
            | Self::ResumeRun { .. }
            | Self::TakeoverRun { .. }
            | Self::ActivateWait { .. }
            | Self::ReleaseEffect { .. }
            | Self::ResolveEffect { .. }
            | Self::CancelRun { .. } => Err(DurableError::Validation(
                "non-page command reached the paged query verifier".to_owned(),
            )),
        }
    }
}

/// Result of one local, exact-key read from a caller-pinned semantic revision.
///
/// This is a Rust read capability, not an Engine wire operation. The owning
/// typed method selects the persistent family and validates both the semantic
/// key and returned value; callers never supply a map root or storage key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableExactRead<T> {
    /// Exact semantic revision that authenticated the requested membership or absence.
    pub observed_revision: String,
    /// Complete verified typed value, or authenticated absence at that exact key.
    pub value: Option<T>,
}

/// Rust-only intent for explicit offline Machine-history maintenance.
///
/// The coordinator derives the causal cut, archive, and exact successor from
/// the revision-pinned Store. Callers supply no Machine snapshot or target state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryCompactionRequest {
    /// Stable maintenance and exact replay identity.
    pub compaction_id: String,
    /// Exact source revision consumed by a fresh compaction.
    pub expected_revision: String,
    /// Closed causal Event-prefix or Event-free admission compaction.
    pub kind: crate::HistoryCompactionKind,
    /// Requested retained Event suffix; Event-free compaction requires zero.
    pub requested_suffix: u64,
}

impl HistoryCompactionRequest {
    /// Validate the maintenance intent before any retained-source traversal.
    ///
    /// # Errors
    /// Returns an error for invalid identity or revision, a suffix outside the
    /// exact integer range, or a nonzero Event-free-compaction suffix.
    pub fn verify(&self) -> DurableResult<()> {
        validate_identity("history compaction", &self.compaction_id)?;
        verify_required_query_revision(&self.expected_revision)?;
        if self.requested_suffix > crate::MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "history compaction suffix exceeds the exact integer range".to_owned(),
            ));
        }
        if self.kind == crate::HistoryCompactionKind::EventFreeAdmissions
            && self.requested_suffix != 0
        {
            return Err(DurableError::Validation(
                "Event-free admission compaction requires a zero suffix".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Store-only durable authority that never constructs or admits an executor.
pub struct DurableStoreControl<S> {
    coordinator: DurableCoordinator<S>,
}

impl<S: DurableStore> DurableStoreControl<S> {
    /// Create the sole parameter-free empty durable domain and return its
    /// provider-independent control.
    ///
    /// # Errors
    /// Returns an error for invalid or already initialized Store authority, storage
    /// failure, or an uncertain initialization publication.
    pub fn initialize(store: S) -> DurableResult<Self> {
        Ok(Self {
            coordinator: DurableCoordinator::open(store)?.initialize()?,
        })
    }

    /// Open the selected store for provider-independent control.
    ///
    /// # Errors
    /// Returns an error if the current Store head and manifest cannot be read or
    /// authenticated.
    pub fn open(store: S) -> DurableResult<Self> {
        Ok(Self {
            coordinator: DurableCoordinator::open(store)?,
        })
    }

    /// Submit one verified provider-independent command.
    ///
    /// # Errors
    /// Returns an error for invalid or unsupported commands, stale or corrupt
    /// authority, or failed or uncertain Store publication.
    pub fn submit(&mut self, command: DurableCommand) -> DurableResult<DurableResponse> {
        command.verify()?;
        if command.is_read_only() {
            return self.coordinator.query(&command);
        }
        match command {
            DurableCommand::ActivateWait {
                activation_id,
                source,
                wait_ids,
                value,
                ..
            } => {
                let receipt = self.coordinator.admit_wait_activation_receipt(
                    activation_id,
                    source,
                    wait_ids,
                    &value,
                )?;
                Ok(DurableResponse::WaitActivated { receipt })
            }
            DurableCommand::CancelRun {
                cancellation_id,
                run_id,
                reason,
                ..
            } => {
                let receipt = self
                    .coordinator
                    .cancel_run(&run_id, &cancellation_id, &reason)?;
                Ok(DurableResponse::RunCancelled { receipt })
            }
            _ => Err(DurableError::Validation(
                "store-only durable control accepts only wait admission, cancellation, and query commands"
                    .to_owned(),
            )),
        }
    }

    /// Return an already retained terminal Effect-resolution receipt without
    /// requiring a provider. `None` means the Effect is still unknown and the
    /// exact historical provider must linearize the requested decision.
    ///
    /// # Errors
    /// Returns an error for a non-resolution command, reused command identity,
    /// invalid retained receipt closure, or storage failure.
    pub fn replay_effect_resolution(
        &mut self,
        command: &DurableCommand,
    ) -> DurableResult<Option<DurableResponse>> {
        command.verify()?;
        let DurableCommand::ResolveEffect {
            resolution_id,
            run_id,
            intent_id,
            execution_binding,
            occurrence_binding,
            claim_owner,
            claim_epoch,
            resolution,
            value,
            ..
        } = command
        else {
            return Err(DurableError::Validation(
                "Effect-resolution replay accepts only resolve_effect".to_owned(),
            ));
        };
        let command = EffectResolutionCommand {
            resolution_id: resolution_id.clone(),
            run_id: run_id.clone(),
            intent_id: intent_id.clone(),
            execution_binding: execution_binding.clone(),
            occurrence_binding: occurrence_binding.clone(),
            claim_owner: claim_owner.clone(),
            claim_epoch: *claim_epoch,
            resolution: *resolution,
            value: value.clone(),
        };
        let Some(receipt) = self.coordinator.effect_resolution_receipt(resolution_id)? else {
            return Ok(None);
        };
        if !receipt.command_matches(&command) {
            return Err(DurableError::HistoryConflict {
                code: "effect_resolution_command_reused".to_owned(),
                message: format!(
                    "Effect resolution identity {resolution_id} was reused with different command semantics"
                ),
            });
        }
        Ok(Some(DurableResponse::EffectResolved { receipt }))
    }

    /// Borrow the closed Resource-profile persistence authority.
    pub fn resource(&mut self) -> DurableResourceControl<'_, S> {
        DurableResourceControl {
            coordinator: &mut self.coordinator,
        }
    }

    /// Borrow the provider-free Agent read authority. Store-only control can
    /// never invoke a host/provider or commit Agent state.
    pub fn agent_read(&mut self) -> DurableAgentReadControl<'_, S> {
        DurableAgentReadControl {
            coordinator: &mut self.coordinator,
        }
    }

    /// Borrow the closed M4 authority with its exact migration/shadow provider
    /// registry. Ordinary Evolution commands require no runtime `PluginHost`,
    /// execution binding, or ambient Clock.
    pub fn evolution<'a>(
        &'a mut self,
        providers: &'a mut dyn evolution_protocol::EvolutionProviders,
    ) -> DurableEvolutionControl<'a, S> {
        DurableEvolutionControl {
            coordinator: &mut self.coordinator,
            providers,
        }
    }

    /// Borrow the provider-free M3 read authority. This view cannot create a
    /// scheduler, acquire a lease, invoke a provider, or publish a CAS.
    pub fn virtual_read(&mut self) -> DurableVirtualReadControl<'_, S> {
        DurableVirtualReadControl {
            coordinator: &mut self.coordinator,
        }
    }

    /// Read one immutable Artifact already referenced by retained application work.
    ///
    /// The exact reference and required revision are checked before lookup. This
    /// operation exposes no Artifact list, raw Machine, or insertion capability.
    ///
    /// # Errors
    /// Returns an error for an invalid exact reference or revision, a stale pin,
    /// corrupt retained Artifact content, or storage failure.
    pub fn read_artifact(
        &mut self,
        reference: &ArtifactRef,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<ArtifactRecord>> {
        reference.validate()?;
        verify_required_query_revision(expected_revision)?;
        let read = self
            .coordinator
            .read_artifact(reference, expected_revision)?;
        verify_exact_read_revision(&read.observed_revision, expected_revision)?;
        if let Some(artifact) = &read.value {
            artifact.validate()?;
            verify_exact_read_key(artifact.reference == *reference)?;
        }
        Ok(read)
    }

    /// Compact one exact source through explicit offline maintenance, or return
    /// the original receipt for an identical retained request.
    ///
    /// This operation may traverse the complete source projection; ordinary
    /// open, queries, and command execution do not inherit that capability.
    ///
    /// # Errors
    /// Returns an error for invalid or reused request semantics, a stale source,
    /// invalid causal cut or archive closure, or failed or uncertain publication.
    pub fn compact_machine_history(
        &mut self,
        request: &HistoryCompactionRequest,
    ) -> DurableResult<crate::HistoryCompactionReceipt> {
        request.verify()?;
        self.coordinator.compact_machine_history(request)
    }

    /// Reconcile the exact head-pinned physical reclamation generation.
    ///
    /// # Errors
    /// Returns an error for a stale head, missing or invalid retained reclamation
    /// authority, or failure to complete its exact authorized deletion page.
    pub fn reconcile_cold_reclamation(&mut self) -> DurableResult<crate::GcReceipt> {
        self.coordinator.reconcile_cold_reclamation()
    }

    /// Publish and reconcile the next bounded physical reclamation generation.
    ///
    /// # Errors
    /// Returns an error for stale or invalid authority, corrupt reachable objects,
    /// an inconsistent reclamation inventory, or storage failure.
    pub fn advance_cold_reclamation(&mut self) -> DurableResult<crate::GcReceipt> {
        self.coordinator.advance_cold_reclamation()
    }

    /// Consume the provider-independent controller and return its store.
    pub fn into_store(self) -> S {
        self.coordinator.into_store()
    }
}

/// Closed Agent-profile persistence authority borrowed from an owning durable
/// control and its exact provider registry.
///
/// Provider products never cross this boundary. The view resolves every
/// source leaf from the pinned `StateRoot`, invokes only the binding retained by
/// that source, and commits the Agent/Resource postcondition in one CAS.
pub struct DurableAgentControl<'a, S> {
    coordinator: &'a mut DurableCoordinator<S>,
    providers: &'a mut dyn agent_protocol::AgentProviders,
    clock: &'a mut dyn crate::ExecutionClockAuthority,
}

/// Provider-free Agent-profile reader borrowed from store-only control.
pub struct DurableAgentReadControl<'a, S> {
    coordinator: &'a mut DurableCoordinator<S>,
}

impl<S: DurableStore> DurableAgentReadControl<'_, S> {
    /// Resolve one revision-pinned M1 workspace admission.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_workspace_admission(
        &mut self,
        query: &agent_protocol::AgentWorkspaceAdmissionQuery,
    ) -> DurableResult<agent_protocol::AgentWorkspaceAdmissionRead> {
        self.coordinator.read_agent_workspace_admission(query)
    }

    /// Read one exact Agent Session current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_session(
        &mut self,
        query: &agent_protocol::AgentSessionQuery,
    ) -> DurableResult<agent_protocol::AgentSessionRead> {
        self.coordinator.read_agent_session(query)
    }

    /// Read one bounded revision-pinned Agent message page.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_messages(
        &mut self,
        query: &agent_protocol::AgentMessagePageQuery,
    ) -> DurableResult<agent_protocol::AgentMessagePageRead> {
        self.coordinator.read_agent_messages(query)
    }

    /// Read one exact Agent message current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_message(
        &mut self,
        query: &agent_protocol::AgentMessageQuery,
    ) -> DurableResult<agent_protocol::AgentMessageRead> {
        self.coordinator.read_agent_message(query)
    }

    /// Read one exact Agent tool current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_tool(
        &mut self,
        query: &agent_protocol::AgentToolQuery,
    ) -> DurableResult<agent_protocol::AgentToolRead> {
        self.coordinator.read_agent_tool(query)
    }

    /// Read one exact Agent elicitation current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_elicitation(
        &mut self,
        query: &agent_protocol::AgentElicitationQuery,
    ) -> DurableResult<agent_protocol::AgentElicitationRead> {
        self.coordinator.read_agent_elicitation(query)
    }

    /// Read one exact Agent occurrence current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_occurrence(
        &mut self,
        query: &agent_protocol::AgentOccurrenceQuery,
    ) -> DurableResult<agent_protocol::AgentOccurrenceRead> {
        self.coordinator.read_agent_occurrence(query)
    }

    /// Read one bounded revision-pinned Agent occurrence page.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_occurrences(
        &mut self,
        query: &agent_protocol::AgentOccurrencePageQuery,
    ) -> DurableResult<agent_protocol::AgentOccurrencePageRead> {
        self.coordinator.read_agent_occurrences(query)
    }

    /// Read one exact Agent stream current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_stream(
        &mut self,
        query: &agent_protocol::AgentStreamQuery,
    ) -> DurableResult<agent_protocol::AgentStreamRead> {
        self.coordinator.read_agent_stream(query)
    }
}

impl<S: DurableStore> DurableAgentControl<'_, S> {
    /// Commit one provider-independent Agent command.
    ///
    /// External stream finalization and workspace dispatch have dedicated
    /// methods so a generic caller cannot bypass their retained provider
    /// bindings.
    ///
    /// # Errors
    /// Returns an error for an invalid command, an action requiring its dedicated
    /// provider boundary, stale authority, or failed or uncertain publication.
    pub fn commit_agent(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> DurableResult<agent_protocol::AgentCommit> {
        command.verify()?;
        match &command.action {
            agent_protocol::AgentCommandAction::Input(_) => {
                self.coordinator.commit_agent_input(command)
            }
            agent_protocol::AgentCommandAction::Stream(
                agent_protocol::AgentStreamCommand::Finalize { .. },
            ) => Err(DurableError::Validation(
                "Agent Finalize must use finalize_agent_stream".to_owned(),
            )),
            agent_protocol::AgentCommandAction::Workspace(_) => Err(DurableError::Validation(
                "Agent Workspace must use commit_agent_workspace".to_owned(),
            )),
            agent_protocol::AgentCommandAction::SessionUpdate { .. }
            | agent_protocol::AgentCommandAction::Occurrence { .. }
            | agent_protocol::AgentCommandAction::Stream(_) => {
                self.coordinator.commit_agent_local(command)
            }
        }
    }

    /// Finalize one staged or external Agent stream. External publication is
    /// obtained from the exact resolver binding retained by the stream.
    ///
    /// # Errors
    /// Returns an error if command, stream, provider, or publication authority is
    /// invalid, or its required Store transition cannot be acknowledged.
    pub fn finalize_agent_stream(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> DurableResult<agent_protocol::AgentStreamFinalizeOutcome> {
        self.coordinator
            .finalize_agent_stream(command, self.providers)
    }

    /// Reconcile one previously ambiguous external stream publication through
    /// read-only provider observation. This path never invokes publication.
    ///
    /// # Errors
    /// Returns an error for an inexact retained intent or provider binding, invalid
    /// observation evidence, or failed or uncertain Store publication.
    pub fn reconcile_agent_stream(
        &mut self,
        command: &agent_protocol::AgentCommand,
        expected_intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> DurableResult<agent_protocol::AgentStreamFinalizeOutcome> {
        self.coordinator
            .reconcile_agent_stream(command, expected_intent, self.providers)
    }

    /// Commit one workspace phase through its exact M1 and provider authority.
    ///
    /// # Errors
    /// Returns an error for invalid command, source, provider, or Clock authority,
    /// rejected preparation, or failed or uncertain Store publication.
    pub fn commit_agent_workspace(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> DurableResult<agent_protocol::AgentWorkspaceCommitOutcome> {
        self.coordinator
            .commit_agent_workspace(command, self.providers, self.clock)
    }

    /// Resolve one exact revision-pinned M1 workspace admission.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_workspace_admission(
        &mut self,
        query: &agent_protocol::AgentWorkspaceAdmissionQuery,
    ) -> DurableResult<agent_protocol::AgentWorkspaceAdmissionRead> {
        self.coordinator.read_agent_workspace_admission(query)
    }

    /// Read one exact Agent Session current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_session(
        &mut self,
        query: &agent_protocol::AgentSessionQuery,
    ) -> DurableResult<agent_protocol::AgentSessionRead> {
        self.coordinator.read_agent_session(query)
    }

    /// Read one bounded revision-pinned Agent message page.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_messages(
        &mut self,
        query: &agent_protocol::AgentMessagePageQuery,
    ) -> DurableResult<agent_protocol::AgentMessagePageRead> {
        self.coordinator.read_agent_messages(query)
    }

    /// Read one exact Agent message current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_message(
        &mut self,
        query: &agent_protocol::AgentMessageQuery,
    ) -> DurableResult<agent_protocol::AgentMessageRead> {
        self.coordinator.read_agent_message(query)
    }

    /// Read one exact Agent tool current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_tool(
        &mut self,
        query: &agent_protocol::AgentToolQuery,
    ) -> DurableResult<agent_protocol::AgentToolRead> {
        self.coordinator.read_agent_tool(query)
    }

    /// Read one exact Agent elicitation current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_elicitation(
        &mut self,
        query: &agent_protocol::AgentElicitationQuery,
    ) -> DurableResult<agent_protocol::AgentElicitationRead> {
        self.coordinator.read_agent_elicitation(query)
    }

    /// Read one exact Agent occurrence current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_occurrence(
        &mut self,
        query: &agent_protocol::AgentOccurrenceQuery,
    ) -> DurableResult<agent_protocol::AgentOccurrenceRead> {
        self.coordinator.read_agent_occurrence(query)
    }

    /// Read one bounded revision-pinned Agent occurrence page.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_occurrences(
        &mut self,
        query: &agent_protocol::AgentOccurrencePageQuery,
    ) -> DurableResult<agent_protocol::AgentOccurrencePageRead> {
        self.coordinator.read_agent_occurrences(query)
    }

    /// Read one exact Agent stream current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_agent_stream(
        &mut self,
        query: &agent_protocol::AgentStreamQuery,
    ) -> DurableResult<agent_protocol::AgentStreamRead> {
        self.coordinator.read_agent_stream(query)
    }
}

/// Closed Resource-profile persistence authority borrowed from an owning
/// durable control.
///
/// This view exposes only Resource semantic commands and typed keyed reads. It
/// cannot access raw Machine, journal, transaction, or `StateRoot` mutation.
pub struct DurableResourceControl<'a, S> {
    coordinator: &'a mut DurableCoordinator<S>,
}

/// Closed M4 persistence authority borrowed from one provider-bound durable
/// runtime.
///
/// The provider registry and target execution binding are fixed by the owning
/// runtime. Commands carry only semantic intent; exact reads, provider products,
/// normalized mutations, and the `StateRoot` CAS remain framework-owned.
pub struct DurableEvolutionControl<'a, S> {
    coordinator: &'a mut DurableCoordinator<S>,
    providers: &'a mut dyn evolution_protocol::EvolutionProviders,
}

impl<S: DurableStore> DurableEvolutionControl<'_, S> {
    /// Commit or exactly replay one closed Evolution persistence command.
    ///
    /// # Errors
    /// Returns an error for invalid or reused command semantics, stale source
    /// authority, rejected provider evidence, or failed or uncertain publication.
    pub fn commit(
        &mut self,
        command: &evolution_protocol::EvolutionPersistenceCommand,
    ) -> DurableResult<evolution_protocol::EvolutionCommit> {
        self.coordinator.commit_evolution(command, self.providers)
    }

    /// Read one exact revision-pinned Evolution scalar current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_current(
        &mut self,
        query: &evolution_protocol::EvolutionCurrentQuery,
    ) -> DurableResult<evolution_protocol::EvolutionCurrentRead> {
        self.coordinator.read_evolution_current(query)
    }

    /// Read one exact all-ever command alias and semantic receipt.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_receipt(
        &mut self,
        query: &evolution_protocol::EvolutionReceiptQuery,
    ) -> DurableResult<evolution_protocol::EvolutionReceiptRead> {
        self.coordinator.read_evolution_receipt(query)
    }

    /// Read the current linked Plan identity for one exact template.
    ///
    /// This observation does not select an occurrence or return executable Plan
    /// bytes. Actual Virtual claims return their own complete pinned Plan.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_template_plan_id(
        &mut self,
        evolution_id: &str,
        template_id: &str,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<String>> {
        validate_identity("Evolution partition", evolution_id)?;
        validate_identity("Evolution template", template_id)?;
        verify_required_query_revision(expected_revision)?;
        let read = self.coordinator.read_evolution_template_plan_id(
            evolution_id,
            template_id,
            expected_revision,
        )?;
        verify_exact_read_revision(&read.observed_revision, expected_revision)?;
        if let Some(plan_id) = &read.value {
            cymule_core::validate_content_id("Evolution template Plan", plan_id)?;
        }
        Ok(read)
    }
}

/// Closed M3 persistence authority borrowed from one provider-bound durable
/// runtime.
pub struct DurableVirtualControl<'a, S> {
    coordinator: &'a mut DurableCoordinator<S>,
    providers: &'a mut dyn virtual_protocol::VirtualProviders,
    clock: &'a mut dyn crate::ExecutionClockAuthority,
    execution_binding: cymule_runtime::ExecutionBinding,
}

/// Provider-free M3 current/receipt reader borrowed from store-only control.
pub struct DurableVirtualReadControl<'a, S> {
    coordinator: &'a mut DurableCoordinator<S>,
}

impl<S: DurableStore> DurableVirtualReadControl<'_, S> {
    /// Read one exact revision-pinned Virtual scalar current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_current(
        &mut self,
        query: &virtual_protocol::VirtualCurrentQuery,
    ) -> DurableResult<virtual_protocol::VirtualCurrentRead> {
        self.coordinator.read_virtual_current(query)
    }

    /// Read one exact all-ever Virtual command receipt.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_receipt(
        &mut self,
        query: &virtual_protocol::VirtualReceiptQuery,
    ) -> DurableResult<virtual_protocol::VirtualReceiptRead> {
        self.coordinator.read_virtual_receipt(query)
    }

    /// Read the exact source, cursor, and lifecycle of one known region.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_region(
        &mut self,
        scheduler_id: &str,
        region_id: &str,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<virtual_protocol::VirtualRegionCurrent>> {
        read_virtual_region(self.coordinator, scheduler_id, region_id, expected_revision)
    }

    /// Read one known work identity and its latest occurrence fence.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_work(
        &mut self,
        scheduler_id: &str,
        work_id: &str,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<virtual_protocol::VirtualWorkCurrent>> {
        read_virtual_work(self.coordinator, scheduler_id, work_id, expected_revision)
    }

    /// Read one exact occurrence referenced by a retained claim or work leaf.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_occurrence(
        &mut self,
        scheduler_id: &str,
        occurrence_id: &str,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<virtual_protocol::VirtualOccurrenceCurrent>> {
        read_virtual_occurrence(
            self.coordinator,
            scheduler_id,
            occurrence_id,
            expected_revision,
        )
    }

    /// Read one known Run's immutable execution selector and fairness state.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_run(
        &mut self,
        scheduler_id: &str,
        run_id: &str,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<virtual_protocol::VirtualRunCurrent>> {
        read_virtual_run(self.coordinator, scheduler_id, run_id, expected_revision)
    }
}

impl<S: DurableStore> DurableVirtualControl<'_, S> {
    /// Commit or exactly replay one closed non-Claim Virtual persistence command.
    ///
    /// An exact retained Claim alias may be replayed as a receipt-only commit.
    /// A fresh Claim must use [`Self::claim`] so the caller receives the complete
    /// [`virtual_protocol::VirtualClaimOutcome`] and its verified Plan.
    ///
    /// # Errors
    /// Returns an error for invalid command, source, binding, provider, or Clock
    /// authority, or failed or uncertain Store publication.
    pub fn commit(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
    ) -> DurableResult<virtual_protocol::VirtualCommit> {
        self.coordinator
            .commit_virtual(command, self.providers, self.clock)
    }

    /// Claim work using this runtime's admitted binding and return executable
    /// semantics only for an actual claim from the same pinned revision.
    ///
    /// # Errors
    /// Returns an error for invalid selection, execution binding, lease, or Clock
    /// authority, rejected preparation, or failed or uncertain publication.
    pub fn claim(
        &mut self,
        command: &virtual_protocol::VirtualClaimPersistenceCommand,
    ) -> DurableResult<virtual_protocol::VirtualClaimOutcome> {
        self.coordinator
            .claim_virtual(command, self.clock, &self.execution_binding)
    }

    /// Read one exact revision-pinned Virtual scalar current.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_current(
        &mut self,
        query: &virtual_protocol::VirtualCurrentQuery,
    ) -> DurableResult<virtual_protocol::VirtualCurrentRead> {
        self.coordinator.read_virtual_current(query)
    }

    /// Read one exact all-ever Virtual command receipt.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_receipt(
        &mut self,
        query: &virtual_protocol::VirtualReceiptQuery,
    ) -> DurableResult<virtual_protocol::VirtualReceiptRead> {
        self.coordinator.read_virtual_receipt(query)
    }

    /// Read the exact source, cursor, and lifecycle of one known region.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_region(
        &mut self,
        scheduler_id: &str,
        region_id: &str,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<virtual_protocol::VirtualRegionCurrent>> {
        read_virtual_region(self.coordinator, scheduler_id, region_id, expected_revision)
    }

    /// Read one known work identity and its latest occurrence fence.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_work(
        &mut self,
        scheduler_id: &str,
        work_id: &str,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<virtual_protocol::VirtualWorkCurrent>> {
        read_virtual_work(self.coordinator, scheduler_id, work_id, expected_revision)
    }

    /// Read one exact occurrence referenced by a retained claim or work leaf.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_occurrence(
        &mut self,
        scheduler_id: &str,
        occurrence_id: &str,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<virtual_protocol::VirtualOccurrenceCurrent>> {
        read_virtual_occurrence(
            self.coordinator,
            scheduler_id,
            occurrence_id,
            expected_revision,
        )
    }

    /// Read one known Run's immutable execution selector and fairness state.
    ///
    /// # Errors
    /// Returns an error for an invalid query, a stale revision, unavailable or
    /// corrupt retained proof, or storage failure.
    pub fn read_run(
        &mut self,
        scheduler_id: &str,
        run_id: &str,
        expected_revision: &str,
    ) -> DurableResult<DurableExactRead<virtual_protocol::VirtualRunCurrent>> {
        read_virtual_run(self.coordinator, scheduler_id, run_id, expected_revision)
    }
}

fn read_virtual_region<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    scheduler_id: &str,
    region_id: &str,
    expected_revision: &str,
) -> DurableResult<DurableExactRead<virtual_protocol::VirtualRegionCurrent>> {
    verify_virtual_exact_query(scheduler_id, "region", region_id, expected_revision)?;
    let read = coordinator.read_virtual_region(scheduler_id, region_id, expected_revision)?;
    verify_exact_read_revision(&read.observed_revision, expected_revision)?;
    if let Some(leaf) = &read.value {
        leaf.verify()?;
        verify_exact_read_key(
            leaf.scheduler_id == scheduler_id && leaf.region.region_id == region_id,
        )?;
    }
    Ok(read)
}

fn read_virtual_work<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    scheduler_id: &str,
    work_id: &str,
    expected_revision: &str,
) -> DurableResult<DurableExactRead<virtual_protocol::VirtualWorkCurrent>> {
    verify_virtual_exact_query(scheduler_id, "work", work_id, expected_revision)?;
    let read = coordinator.read_virtual_work(scheduler_id, work_id, expected_revision)?;
    verify_exact_read_revision(&read.observed_revision, expected_revision)?;
    if let Some(leaf) = &read.value {
        leaf.verify()?;
        verify_exact_read_key(leaf.scheduler_id == scheduler_id && leaf.item.work_id == work_id)?;
    }
    Ok(read)
}

fn read_virtual_occurrence<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    scheduler_id: &str,
    occurrence_id: &str,
    expected_revision: &str,
) -> DurableResult<DurableExactRead<virtual_protocol::VirtualOccurrenceCurrent>> {
    verify_virtual_exact_query(scheduler_id, "occurrence", occurrence_id, expected_revision)?;
    cymule_core::validate_content_id("Virtual occurrence", occurrence_id)?;
    let read =
        coordinator.read_virtual_occurrence(scheduler_id, occurrence_id, expected_revision)?;
    verify_exact_read_revision(&read.observed_revision, expected_revision)?;
    if let Some(leaf) = &read.value {
        leaf.verify()?;
        verify_exact_read_key(
            leaf.scheduler_id == scheduler_id && leaf.occurrence.occurrence_id == occurrence_id,
        )?;
    }
    Ok(read)
}

fn read_virtual_run<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    scheduler_id: &str,
    run_id: &str,
    expected_revision: &str,
) -> DurableResult<DurableExactRead<virtual_protocol::VirtualRunCurrent>> {
    verify_virtual_exact_query(scheduler_id, "Run", run_id, expected_revision)?;
    let read = coordinator.read_virtual_run(scheduler_id, run_id, expected_revision)?;
    verify_exact_read_revision(&read.observed_revision, expected_revision)?;
    if let Some(leaf) = &read.value {
        leaf.verify()?;
        verify_exact_read_key(leaf.scheduler_id == scheduler_id && leaf.run_id == run_id)?;
    }
    Ok(read)
}

fn verify_virtual_exact_query(
    scheduler_id: &str,
    key_kind: &str,
    semantic_id: &str,
    expected_revision: &str,
) -> DurableResult<()> {
    validate_identity("Virtual scheduler", scheduler_id)?;
    validate_identity(&format!("Virtual {key_kind}"), semantic_id)?;
    verify_required_query_revision(expected_revision)
}

fn verify_required_query_revision(revision: &str) -> DurableResult<()> {
    cymule_core::validate_content_id("durable exact query expected revision", revision)?;
    Ok(())
}

fn verify_exact_read_revision(observed: &str, expected: &str) -> DurableResult<()> {
    cymule_core::validate_content_id("durable exact query observed revision", observed)?;
    if observed != expected {
        return Err(DurableError::Integrity {
            code: "durable_exact_query_revision_mismatch".to_owned(),
            message: "exact query returned a different physical revision".to_owned(),
        });
    }
    Ok(())
}

fn verify_exact_read_key(matches: bool) -> DurableResult<()> {
    if !matches {
        return Err(DurableError::Integrity {
            code: "durable_exact_query_key_mismatch".to_owned(),
            message: "exact query returned a different semantic owner or identity".to_owned(),
        });
    }
    Ok(())
}

impl<S: DurableStore> DurableResourceControl<'_, S> {
    /// Commit one verified Resource semantic command.
    ///
    /// # Errors
    /// Returns an error for invalid command or retained Resource authority, a stale
    /// source, or failed or uncertain Store publication.
    pub fn commit(
        &mut self,
        command: &resource_protocol::ResourceCommand,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        self.coordinator.commit_resource(command)
    }

    /// Reconcile one exact deletion through its provider binding.
    ///
    /// # Errors
    /// Returns an error for invalid deletion authority or provider evidence, a stale
    /// source, or failed or uncertain Store publication.
    pub fn reconcile_delete(
        &mut self,
        command: &resource_protocol::ResourceCommand,
        deleter: &mut impl resource_protocol::ResourceDeleter,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        self.coordinator.reconcile_resource_delete(command, deleter)
    }

    /// Resolve one exact immutable Resource command receipt.
    ///
    /// # Errors
    /// Returns an error for invalid lookup or page bounds, unavailable or corrupt
    /// retained authority, or storage failure.
    pub fn command_receipt(
        &mut self,
        command_id: &str,
    ) -> DurableResult<Option<resource_protocol::ResourceCommandReceipt>> {
        self.coordinator.resource_command_receipt(command_id)
    }

    /// Resolve one exact current Resource pin projection.
    ///
    /// # Errors
    /// Returns an error for invalid lookup or page bounds, unavailable or corrupt
    /// retained authority, or storage failure.
    pub fn pin_current(
        &mut self,
        pin_id: &str,
    ) -> DurableResult<Option<resource_protocol::ResourcePinCurrent>> {
        self.coordinator.resource_pin_current(pin_id)
    }

    /// Resolve one exact current physical Resource retention projection.
    ///
    /// # Errors
    /// Returns an error for invalid lookup or page bounds, unavailable or corrupt
    /// retained authority, or storage failure.
    pub fn retention_current(
        &mut self,
        retention_key: &str,
    ) -> DurableResult<Option<resource_protocol::ResourceRetentionCurrent>> {
        self.coordinator.resource_retention_current(retention_key)
    }

    /// Resolve one exact current Resource deletion projection.
    ///
    /// # Errors
    /// Returns an error for invalid lookup or page bounds, unavailable or corrupt
    /// retained authority, or storage failure.
    pub fn delete_current(
        &mut self,
        delete_id: &str,
    ) -> DurableResult<Option<resource_protocol::ResourceDeleteCurrent>> {
        self.coordinator.resource_delete_current(delete_id)
    }

    /// Resolve one exact immutable Resource handoff authority.
    ///
    /// # Errors
    /// Returns an error for invalid lookup or page bounds, unavailable or corrupt
    /// retained authority, or storage failure.
    pub fn handoff_current(
        &mut self,
        transfer_id: &str,
    ) -> DurableResult<Option<resource_protocol::ResourceHandoffCurrent>> {
        self.coordinator.resource_handoff_current(transfer_id)
    }

    /// Resolve one exact immutable Resource handoff activation authority.
    ///
    /// # Errors
    /// Returns an error for invalid lookup or page bounds, unavailable or corrupt
    /// retained authority, or storage failure.
    pub fn handoff_activation_current(
        &mut self,
        activation_id: &str,
    ) -> DurableResult<Option<resource_protocol::ResourceHandoffActivationCurrent>> {
        self.coordinator
            .resource_handoff_activation_current(activation_id)
    }

    /// Resolve one bounded contiguous page of handoffs for a target Run.
    ///
    /// # Errors
    /// Returns an error for invalid lookup or page bounds, unavailable or corrupt
    /// retained authority, or storage failure.
    pub fn handoff_page(
        &mut self,
        to_run: &str,
        start_index: u64,
        limit: usize,
    ) -> DurableResult<resource_protocol::ResourceHandoffPage> {
        self.coordinator
            .resource_handoff_page(to_run, start_index, limit)
    }
}

/// Provider-only control for terminal Effect settlement. It owns no Clock and
/// cannot start, resume, release, or take over a Run.
pub struct DurableProviderControl<S, P> {
    runtime: EffectResolutionRuntime<S, P>,
}

impl<S: DurableStore, P: BoundPluginHost> DurableProviderControl<S, P> {
    /// Open provider-linearized settlement over one exact admitted binding.
    ///
    /// # Errors
    /// Returns an error if the Store's retained authority cannot be opened under
    /// the supplied admitted execution binding.
    pub fn open(store: S, admission: ExecutionBindingAdmission<P>) -> DurableResult<Self> {
        Ok(Self {
            runtime: EffectResolutionRuntime::open(store, admission)?,
        })
    }

    /// Submit the sole provider-linearized settlement command.
    ///
    /// # Errors
    /// Returns an error for a non-resolution command, invalid historical authority,
    /// a rejected provider observation, or failed or uncertain receipt publication.
    pub fn submit(&mut self, command: DurableCommand) -> DurableResult<DurableResponse> {
        command.verify()?;
        let DurableCommand::ResolveEffect {
            resolution_id,
            run_id,
            intent_id,
            execution_binding,
            occurrence_binding,
            claim_owner,
            claim_epoch,
            resolution,
            value,
            ..
        } = command
        else {
            return Err(DurableError::Validation(
                "provider-only durable control accepts only resolve_effect".to_owned(),
            ));
        };
        let receipt = self
            .runtime
            .resolve_effect_with_provider(&EffectResolutionCommand {
                resolution_id,
                run_id,
                intent_id,
                execution_binding,
                occurrence_binding,
                claim_owner,
                claim_epoch,
                resolution,
                value,
            })?;
        Ok(DurableResponse::EffectResolved { receipt })
    }

    /// Consume the controller into its Store and admitted provider.
    pub fn into_parts(self) -> (S, P) {
        self.runtime.into_parts()
    }
}

/// Serializable Run boundary returned by mutation controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableBoundary {
    /// The Run parked at one durable wait.
    Suspended {
        /// Stable wait identity.
        wait_id: String,
    },
    /// An effect remains ambiguous and requires reconciliation.
    ReconciliationRequired {
        /// Original structural effect intent.
        intent_id: String,
    },
    /// The original effect handler is unavailable and governance must resolve
    /// the retained world outcome.
    EffectUnavailable {
        /// Original structural effect intent.
        intent_id: String,
    },
    /// A bound eager effect settled as `NotApplied` and produced no bindable
    /// value. The Run remains at the effect site without an execution claim.
    EffectNotApplied {
        /// Original structural effect intent.
        intent_id: String,
    },
    /// One or more explicit effects require caller release.
    ReleaseRequired {
        /// Stable structural effect intents.
        intent_ids: BTreeSet<String>,
    },
    /// The Run committed its terminal result.
    Completed {
        /// Full execution result and replay evidence.
        result: ExecutionResult,
    },
    /// The Run committed one typed terminal failure.
    Failed {
        /// Canonical Run failure.
        failure: RunFailure,
    },
    /// The Run committed one semantic cancellation.
    Cancelled {
        /// Content-addressed cancellation reason.
        reason: ArtifactRef,
    },
}

impl From<DriveOutcome> for DurableBoundary {
    fn from(outcome: DriveOutcome) -> Self {
        match outcome {
            DriveOutcome::Suspended { wait_id } => Self::Suspended { wait_id },
            DriveOutcome::ReconciliationRequired { intent_id } => {
                Self::ReconciliationRequired { intent_id }
            }
            DriveOutcome::EffectUnavailable { intent_id } => Self::EffectUnavailable { intent_id },
            DriveOutcome::EffectNotApplied { intent_id } => Self::EffectNotApplied { intent_id },
            DriveOutcome::ReleaseRequired { intent_ids } => Self::ReleaseRequired { intent_ids },
            DriveOutcome::Completed(result) => Self::Completed { result },
            DriveOutcome::Failed { failure } => Self::Failed { failure },
            DriveOutcome::Cancelled { reason } => Self::Cancelled { reason },
        }
    }
}

/// Closed query families whose authenticated map order may be continued by a
/// [`DurablePageCursor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurablePageQueryKind {
    /// Domain Run index.
    RunIndex,
    /// Waits owned by one Run.
    RunWaits,
    /// Effects owned by one Run.
    RunEffects,
    /// Component occurrences owned by one Run.
    RunOccurrences,
    /// Provider Attempts owned by one Run.
    RunAttempts,
}

impl DurablePageQueryKind {
    const fn is_run_scoped(self) -> bool {
        !matches!(self, Self::RunIndex)
    }

    fn verify_key(self, canonical_key: &str) -> DurableResult<()> {
        match self {
            Self::RunIndex => {
                validate_identity("Run", canonical_key)?;
                if canonical_key.chars().count() > MAX_DURABLE_QUERY_RUN_KEY_SCALARS {
                    return Err(DurableError::Validation(format!(
                        "durable Run query key exceeds {MAX_DURABLE_QUERY_RUN_KEY_SCALARS} Unicode scalar values"
                    )));
                }
                Ok(())
            }
            Self::RunWaits => crate::model::validate_sha256_identity("wait", canonical_key),
            Self::RunEffects => {
                crate::model::validate_sha256_identity("effect intent", canonical_key)
            }
            Self::RunOccurrences => {
                crate::model::validate_sha256_identity("component occurrence", canonical_key)
            }
            Self::RunAttempts => {
                crate::model::validate_sha256_identity("operation Attempt", canonical_key)
            }
        }
    }
}

/// Complete authenticated hash-trie position consumed by the next page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurablePagePosition {
    /// Exact canonical map key of the final item returned by the prior page.
    pub canonical_key: String,
    /// Exact `StateMap` hash of `canonical_key`.
    pub key_hash: String,
}

impl DurablePagePosition {
    pub(crate) fn for_key(canonical_key: &str) -> DurableResult<Self> {
        validate_identity("query cursor key", canonical_key)?;
        Ok(Self {
            canonical_key: canonical_key.to_owned(),
            key_hash: durable_page_key_hash(canonical_key)?,
        })
    }

    /// Validate the complete key/hash tuple.
    ///
    /// # Errors
    /// Returns an error for an invalid key or digest, or a key/hash mismatch.
    pub fn verify(&self) -> DurableResult<()> {
        validate_identity("query cursor key", &self.canonical_key)?;
        validate_raw_digest("query cursor key hash", &self.key_hash)?;
        if self.key_hash != durable_page_key_hash(&self.canonical_key)? {
            return Err(DurableError::Validation(
                "durable query cursor key and key hash disagree".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Opaque continuation for one exact revision/root/query/owner tuple.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurablePageCursor {
    /// Closed query family.
    pub query_kind: DurablePageQueryKind,
    /// Owning Run for Run-scoped pages; required on wire as null for Run index pages.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub run_id: Option<String>,
    /// Exact semantic revision observed by the first page.
    pub source_revision: String,
    /// Canonical digest of the complete source `MapRoot`.
    pub source_root: String,
    /// Full final-item continuation position.
    pub position: DurablePagePosition,
}

impl DurablePageCursor {
    pub(crate) fn new(
        query_kind: DurablePageQueryKind,
        run_id: Option<&str>,
        source_revision: &str,
        source_root: &str,
        canonical_key: &str,
    ) -> DurableResult<Self> {
        let cursor = Self {
            query_kind,
            run_id: run_id.map(str::to_owned),
            source_revision: source_revision.to_owned(),
            source_root: source_root.to_owned(),
            position: DurablePagePosition::for_key(canonical_key)?,
        };
        cursor.verify()?;
        Ok(cursor)
    }

    /// Validate one self-contained cursor independently of current Store state.
    ///
    /// # Errors
    /// Returns an error for invalid owner, revision, root, or position identities,
    /// including a query-family and owner mismatch.
    pub fn verify(&self) -> DurableResult<()> {
        if self.query_kind.is_run_scoped() != self.run_id.is_some() {
            return Err(DurableError::Validation(
                "durable query cursor owner does not match its query kind".to_owned(),
            ));
        }
        if let Some(run_id) = &self.run_id {
            validate_identity("Run", run_id)?;
        }
        cymule_core::validate_content_id("durable query source revision", &self.source_revision)?;
        validate_raw_digest("durable query source root", &self.source_root)?;
        self.position.verify()?;
        self.query_kind
            .verify_key(self.position.canonical_key.as_str())
    }

    fn verify_scope(
        &self,
        query_kind: DurablePageQueryKind,
        run_id: Option<&str>,
    ) -> DurableResult<()> {
        self.verify()?;
        if self.query_kind != query_kind || self.run_id.as_deref() != run_id {
            return Err(DurableError::Validation(
                "durable query cursor belongs to a different query or Run".to_owned(),
            ));
        }
        Ok(())
    }
}

/// One bounded revision/root-pinned page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableQueryPage<T> {
    /// Exact semantic revision observed while resolving every item.
    pub observed_revision: String,
    /// Canonical digest of the complete source `MapRoot`.
    pub source_root: String,
    /// Bounded semantic summaries in authenticated map-key order.
    pub items: Vec<T>,
    /// Continuation for a non-terminal page, required on wire as null when terminal.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub next_cursor: Option<DurablePageCursor>,
}

impl<T: Serialize> DurableQueryPage<T> {
    fn verify(
        &self,
        query_kind: DurablePageQueryKind,
        run_id: Option<&str>,
        item_key: impl for<'a> Fn(&'a T) -> &'a str,
        verify_item: impl Fn(&T, Option<&str>) -> DurableResult<()>,
    ) -> DurableResult<()> {
        cymule_core::validate_content_id(
            "durable query observed revision",
            &self.observed_revision,
        )?;
        validate_raw_digest("durable query source root", &self.source_root)?;
        if u32::try_from(self.items.len())
            .map_or(true, |count| count > MAX_DURABLE_QUERY_PAGE_ITEMS)
        {
            return Err(DurableError::Validation(format!(
                "durable query page exceeds {MAX_DURABLE_QUERY_PAGE_ITEMS} items"
            )));
        }
        let mut previous: Option<DurablePagePosition> = None;
        for item in &self.items {
            verify_item(item, run_id)?;
            let canonical_key = item_key(item);
            query_kind.verify_key(canonical_key)?;
            let position = DurablePagePosition::for_key(canonical_key)?;
            if previous.as_ref().is_some_and(|previous| {
                (previous.key_hash.as_str(), previous.canonical_key.as_str())
                    >= (position.key_hash.as_str(), position.canonical_key.as_str())
            }) {
                return Err(DurableError::Validation(
                    "durable query page items are not in strict authenticated key order".to_owned(),
                ));
            }
            previous = Some(position);
        }
        if let Some(cursor) = &self.next_cursor {
            cursor.verify_scope(query_kind, run_id)?;
            if cursor.source_revision != self.observed_revision
                || cursor.source_root != self.source_root
                || previous.as_ref() != Some(&cursor.position)
            {
                return Err(DurableError::Validation(
                    "durable query next cursor does not bind the page's exact terminal item, revision, and root"
                        .to_owned(),
                ));
            }
        }
        verify_canonical_size(
            "durable query page",
            self,
            query_byte_limit_usize(MAX_DURABLE_QUERY_PAGE_BYTES)?,
        )
    }
}

/// Small domain-index summary for one Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableRunIndexSummary {
    /// Stable Run identity.
    pub run_id: String,
    /// Current resumable lifecycle, without the full Continuation.
    pub continuation_status: ContinuationStatus,
    /// Canonical execution axis.
    pub execution_status: RunExecutionStatus,
    /// External-world settlement axis.
    pub world_settlement: WorldSettlementStatus,
}

impl DurableRunIndexSummary {
    /// Validate one bounded Run-index summary.
    ///
    /// # Errors
    /// Returns an error for an invalid Run identity, inconsistent execution or
    /// settlement axes, or a summary exceeding the canonical byte bound.
    pub fn verify(&self) -> DurableResult<()> {
        validate_identity("Run", &self.run_id)?;
        verify_continuation_execution_axes(self.continuation_status, &self.execution_status)?;
        verify_run_axes(&self.execution_status, self.world_settlement, None, false)?;
        verify_summary_size("Run-index summary", self)
    }
}

/// Bounded semantic current projection for one Run. It intentionally excludes
/// physical child roots, execution frames, wait sets, and every child collection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableRunCurrent {
    /// Stable Run identity.
    pub run_id: String,
    /// Current immutable Plan identity.
    pub plan_id: String,
    /// Exact typed execution-binding Artifact.
    pub execution_binding: ArtifactRef,
    /// Current resumable lifecycle, without the full Continuation.
    pub continuation_status: ContinuationStatus,
    /// Current semantic execution epoch.
    pub epoch: u64,
    /// Current execution fence.
    pub execution_fence: u64,
    /// Optional canonical terminal result Artifact, required on wire as null when absent.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub result: Option<ArtifactRef>,
    /// Canonical execution axis.
    pub execution_status: RunExecutionStatus,
    /// External-world settlement axis.
    pub world_settlement: WorldSettlementStatus,
}

impl DurableRunCurrent {
    /// Validate one bounded semantic Run projection.
    ///
    /// # Errors
    /// Returns an error for invalid Run, Plan, binding, or fencing metadata,
    /// inconsistent lifecycle or result, or an oversized current projection.
    pub fn verify(&self) -> DurableResult<()> {
        validate_identity("Run", &self.run_id)?;
        crate::model::validate_sha256_identity("Run Plan", &self.plan_id)?;
        self.execution_binding
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if self.execution_binding.kind != cymule_runtime::EXECUTION_BINDING_VERSION {
            return Err(DurableError::Validation(
                "Run current requires a cymule.execution-binding/2 Artifact".to_owned(),
            ));
        }
        if self.epoch > crate::MAX_EXACT_INTEGER || self.execution_fence > crate::MAX_EXACT_INTEGER
        {
            return Err(DurableError::Validation(
                "Run current epoch or execution fence exceeds the exact integer range".to_owned(),
            ));
        }
        verify_continuation_execution_axes(self.continuation_status, &self.execution_status)?;
        verify_run_axes(
            &self.execution_status,
            self.world_settlement,
            self.result.as_ref(),
            true,
        )?;
        verify_summary_size("Run-current projection", self)
    }
}

/// Small page summary for one Run-owned wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableWaitSummary {
    /// Stable wait identity.
    pub wait_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Current wait lifecycle.
    pub state: WaitState,
    /// Completion Artifact, required on wire as null when absent.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub result: Option<ArtifactRef>,
}

impl DurableWaitSummary {
    /// Derive the bounded query projection from one already verified complete Wait.
    pub(crate) fn from_wait(wait: &WaitCondition) -> Self {
        Self {
            wait_id: wait.wait_id.clone(),
            run_id: wait.run_id.clone(),
            state: wait.state,
            result: wait.result.clone(),
        }
    }

    /// Validate one bounded wait summary.
    ///
    /// # Errors
    /// Returns an error for invalid identities, inconsistent wait state and result,
    /// or an oversized summary.
    pub fn verify(&self) -> DurableResult<()> {
        crate::model::validate_sha256_identity("wait", &self.wait_id)?;
        validate_identity("Run", &self.run_id)?;
        match (self.state, &self.result) {
            (WaitState::Completed, Some(result)) => result
                .validate()
                .map_err(|error| DurableError::Validation(error.to_string()))?,
            (WaitState::Pending | WaitState::Cancelled, None) => {}
            _ => {
                return Err(DurableError::Validation(
                    "wait summary lifecycle does not match its result".to_owned(),
                ));
            }
        }
        verify_summary_size("wait summary", self)
    }
}

/// Small page summary for one Run-owned Effect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableEffectSummary {
    /// Structural Effect intent identity.
    pub intent_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Current outbox lifecycle.
    pub state: OutboxState,
    /// Exact historical implementation availability.
    pub execution_availability: cymule_core::EffectExecutionAvailability,
    /// Canonical reconciliation axis.
    pub reconciliation: cymule_core::ReconciliationState,
    /// Authoritative result Artifact, required on wire as null when absent.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub result: Option<ArtifactRef>,
}

impl DurableEffectSummary {
    /// Validate one bounded Effect summary.
    ///
    /// # Errors
    /// Returns an error for invalid identities or result, inconsistent Effect
    /// lifecycle or reconciliation, or an oversized summary.
    pub fn verify(&self) -> DurableResult<()> {
        crate::model::validate_sha256_identity("effect intent", &self.intent_id)?;
        validate_identity("Run", &self.run_id)?;
        if let Some(result) = &self.result {
            result
                .validate()
                .map_err(|error| DurableError::Validation(error.to_string()))?;
            if result.kind != crate::model::EFFECT_RESULT_ARTIFACT_KIND {
                return Err(DurableError::Validation(
                    "Effect summary result has the wrong Artifact kind".to_owned(),
                ));
            }
        }
        let reconciliation_matches = match self.state {
            OutboxState::Pending | OutboxState::Claimed => {
                self.reconciliation == cymule_core::ReconciliationState::NotRequired
            }
            OutboxState::Applied | OutboxState::NotApplied => matches!(
                self.reconciliation,
                cymule_core::ReconciliationState::NotRequired
                    | cymule_core::ReconciliationState::Resolved
            ),
            OutboxState::Unknown => matches!(
                self.reconciliation,
                cymule_core::ReconciliationState::Pending
                    | cymule_core::ReconciliationState::GovernanceRequired
            ),
            OutboxState::CancelledBeforeRelease => {
                self.reconciliation == cymule_core::ReconciliationState::Resolved
            }
        };
        if !reconciliation_matches
            || self.state == OutboxState::Applied && self.result.is_none()
            || matches!(
                self.state,
                OutboxState::Pending
                    | OutboxState::Claimed
                    | OutboxState::Unknown
                    | OutboxState::NotApplied
                    | OutboxState::CancelledBeforeRelease
            ) && self.result.is_some()
            || matches!(self.state, OutboxState::Pending | OutboxState::Claimed)
                && self.execution_availability
                    != cymule_core::EffectExecutionAvailability::Available
        {
            return Err(DurableError::Validation(
                "Effect summary lifecycle is inconsistent".to_owned(),
            ));
        }
        verify_summary_size("Effect summary", self)
    }
}

/// Small page summary for one Run-owned component occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableOccurrenceSummary {
    /// Structural occurrence identity.
    pub occurrence_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Current occurrence lifecycle.
    pub state: ComponentOccurrenceState,
    /// Terminal outcome, required on wire as null while pending.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub outcome: Option<ComponentOutcome>,
}

impl DurableOccurrenceSummary {
    /// Validate one bounded occurrence summary.
    ///
    /// # Errors
    /// Returns an error for invalid identities or outcome, inconsistent occurrence
    /// lifecycle, or an oversized summary.
    pub fn verify(&self) -> DurableResult<()> {
        crate::model::validate_sha256_identity("component occurrence", &self.occurrence_id)?;
        validate_identity("Run", &self.run_id)?;
        if let Some(outcome) = &self.outcome {
            outcome.verify_wire()?;
        }
        if matches!(self.state, ComponentOccurrenceState::Pending) != self.outcome.is_none() {
            return Err(DurableError::Validation(
                "component occurrence summary lifecycle is inconsistent".to_owned(),
            ));
        }
        verify_summary_size("component occurrence summary", self)
    }
}

/// Small page summary for one Run-owned provider Attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableAttemptSummary {
    /// Content-addressed Attempt identity.
    pub attempt_id: String,
    /// Stable semantic occurrence identity.
    pub occurrence_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Monotonic Attempt ordinal within the occurrence.
    pub attempt_ordinal: u64,
    /// Current Attempt lifecycle.
    pub state: OperationAttemptState,
    /// Terminal provider outcome, required on wire as null before completion.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub outcome: Option<ComponentOutcome>,
}

impl DurableAttemptSummary {
    /// Validate one bounded Attempt summary.
    ///
    /// # Errors
    /// Returns an error for invalid identities, ordinal, or outcome, inconsistent
    /// attempt lifecycle, or an oversized summary.
    pub fn verify(&self) -> DurableResult<()> {
        crate::model::validate_sha256_identity("operation Attempt", &self.attempt_id)?;
        crate::model::validate_sha256_identity("component occurrence", &self.occurrence_id)?;
        validate_identity("Run", &self.run_id)?;
        if self.attempt_ordinal == 0 || self.attempt_ordinal > crate::MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "operation Attempt summary ordinal is outside the exact positive integer range"
                    .to_owned(),
            ));
        }
        if let Some(outcome) = &self.outcome {
            outcome.verify_wire()?;
        }
        match self.state {
            OperationAttemptState::Completed if self.outcome.is_some() => {}
            OperationAttemptState::Running | OperationAttemptState::Superseded
                if self.outcome.is_none() => {}
            _ => {
                return Err(DurableError::Validation(
                    "operation Attempt summary lifecycle is inconsistent".to_owned(),
                ));
            }
        }
        verify_summary_size("operation Attempt summary", self)
    }
}

/// Closed selector for one exact Run-owned typed leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableRunItemSelector {
    /// Exact wait.
    Wait {
        /// Exact wait identity.
        wait_id: String,
    },
    /// Exact Effect.
    Effect {
        /// Exact structural Effect intent.
        intent_id: String,
    },
    /// Exact component occurrence.
    Occurrence {
        /// Exact component-occurrence identity.
        occurrence_id: String,
    },
    /// Exact provider Attempt.
    Attempt {
        /// Exact provider-attempt identity.
        attempt_id: String,
    },
}

impl DurableRunItemSelector {
    /// Validate one exact typed selector.
    ///
    /// # Errors
    /// Returns an error if the selected typed identity is not a valid content ID.
    pub fn verify(&self) -> DurableResult<()> {
        let (kind, identity) = match self {
            Self::Wait { wait_id } => ("wait", wait_id),
            Self::Effect { intent_id } => ("effect intent", intent_id),
            Self::Occurrence { occurrence_id } => ("component occurrence", occurrence_id),
            Self::Attempt { attempt_id } => ("operation Attempt", attempt_id),
        };
        crate::model::validate_sha256_identity(kind, identity)
    }
}

/// One complete exact Run-owned typed leaf. Large payloads are reachable only
/// through this read and never repeated inside a page summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableRunItem {
    /// Complete wait leaf.
    Wait {
        /// Exact retained wait.
        wait: Box<WaitCondition>,
    },
    /// Complete Effect leaf.
    Effect {
        /// Exact retained Effect dispatch.
        effect: Box<EffectDispatch>,
    },
    /// Complete component occurrence leaf.
    Occurrence {
        /// Exact retained component occurrence.
        occurrence: Box<ComponentOccurrence>,
    },
    /// Complete provider Attempt leaf.
    Attempt {
        /// Exact retained provider attempt.
        attempt: Box<OperationAttempt>,
    },
}

impl DurableRunItem {
    /// Validate one exact leaf independently of `StateRoot` retention.
    ///
    /// # Errors
    /// Returns an error if the selected leaf is malformed, internally inconsistent,
    /// or exceeds the canonical leaf bound.
    pub fn verify(&self) -> DurableResult<()> {
        match self {
            Self::Wait { wait } => {
                wait.verify_wire()?;
                verify_canonical_size(
                    "exact durable wait leaf",
                    wait.as_ref(),
                    crate::MAX_STATE_ROOT_LEAF_BYTES,
                )?;
            }
            Self::Effect { effect } => {
                effect.verify_wire()?;
                verify_canonical_size(
                    "exact durable Effect leaf",
                    effect.as_ref(),
                    crate::MAX_STATE_ROOT_LEAF_BYTES,
                )?;
            }
            Self::Occurrence { occurrence } => {
                occurrence.verify()?;
                verify_canonical_size(
                    "exact durable component-occurrence leaf",
                    occurrence.as_ref(),
                    crate::MAX_STATE_ROOT_LEAF_BYTES,
                )?;
            }
            Self::Attempt { attempt } => {
                attempt.verify()?;
                verify_canonical_size(
                    "exact durable operation-Attempt leaf",
                    attempt.as_ref(),
                    crate::MAX_STATE_ROOT_LEAF_BYTES,
                )?;
            }
        }
        Ok(())
    }

    /// Owning Run retained by the exact leaf.
    pub fn run_id(&self) -> &str {
        match self {
            Self::Wait { wait } => &wait.run_id,
            Self::Effect { effect } => &effect.run_id,
            Self::Occurrence { occurrence } => &occurrence.run_id,
            Self::Attempt { attempt } => &attempt.run_id,
        }
    }
}

/// Domain Run-index page.
pub type DurableRunIndexPage = DurableQueryPage<DurableRunIndexSummary>;
/// One Run's wait-summary page.
pub type DurableRunWaitPage = DurableQueryPage<DurableWaitSummary>;
/// One Run's Effect-summary page.
pub type DurableRunEffectPage = DurableQueryPage<DurableEffectSummary>;
/// One Run's component-occurrence-summary page.
pub type DurableRunOccurrencePage = DurableQueryPage<DurableOccurrenceSummary>;
/// One Run's provider-Attempt-summary page.
pub type DurableRunAttemptPage = DurableQueryPage<DurableAttemptSummary>;

/// Complete normalized semantic command retained by one Run-cancellation
/// receipt. The outer durable-control version and variant tag select decoding;
/// these fields are the immutable idempotency semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancellationCommand {
    /// Stable cancellation and idempotency identity.
    pub cancellation_id: String,
    /// Run to cancel.
    pub run_id: String,
    /// Exact provider-neutral semantic reason.
    pub reason: Value,
}

impl CancellationCommand {
    /// Validate the normalized command independently of durable state.
    ///
    /// # Errors
    /// Returns an error if the cancellation or Run identity is invalid.
    pub fn verify(&self) -> DurableResult<()> {
        validate_identity("cancellation", &self.cancellation_id)?;
        validate_identity("Run", &self.run_id)
    }
}

/// Complete normalized semantic command retained by one terminal Effect
/// resolution receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectResolutionCommand {
    /// Stable resolution and idempotency identity.
    pub resolution_id: String,
    /// Owning Run retained by the original intent.
    pub run_id: String,
    /// Structural Effect intent identity.
    pub intent_id: String,
    /// Exact historical execution-binding Artifact.
    pub execution_binding: ArtifactRef,
    /// Exact occurrence binding derived from that Artifact.
    pub occurrence_binding: String,
    /// Original dispatch-claim owner retained after ambiguity.
    pub claim_owner: String,
    /// Original dispatch-claim fence retained after ambiguity.
    pub claim_epoch: u64,
    /// Requested terminal world resolution.
    pub resolution: ReconciliationResolution,
    /// Requested authoritative output, explicitly null when absent.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub value: Option<Value>,
}

impl EffectResolutionCommand {
    /// Validate the normalized command independently of durable state.
    ///
    /// # Errors
    /// Returns an error for invalid identity, binding, or claim authority, a
    /// nonterminal resolution, or a result attached to a `NotApplied` request.
    pub fn verify(&self) -> DurableResult<()> {
        validate_identity("effect resolution", &self.resolution_id)?;
        validate_identity("Run", &self.run_id)?;
        crate::model::validate_sha256_identity("effect intent", &self.intent_id)?;
        self.execution_binding
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if self.execution_binding.kind != cymule_runtime::EXECUTION_BINDING_VERSION {
            return Err(DurableError::Validation(
                "Effect resolution requires a cymule.execution-binding/2 Artifact".to_owned(),
            ));
        }
        crate::model::validate_sha256_identity(
            "effect occurrence binding",
            &self.occurrence_binding,
        )?;
        validate_identity("effect claim owner", &self.claim_owner)?;
        if self.claim_epoch == 0 || self.claim_epoch > crate::MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "Effect resolution claim epoch must use the exact positive cross-language range"
                    .to_owned(),
            ));
        }
        if !is_terminal_resolution(self.resolution) {
            return Err(DurableError::Validation(
                "Effect resolution must be resolved_applied or resolved_not_applied".to_owned(),
            ));
        }
        if self.resolution == ReconciliationResolution::ResolvedNotApplied && self.value.is_some() {
            return Err(DurableError::Validation(
                "NotApplied resolution cannot carry an Effect result".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Exact receipt for one provider-independent terminal Effect resolution.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectResolutionReceipt {
    /// Frozen receipt generation.
    pub receipt_version: String,
    /// Complete immutable command accepted before provider linearization.
    pub command: EffectResolutionCommand,
    /// Independent terminal world resolution returned by the historical provider.
    pub actual_resolution: ReconciliationResolution,
    /// Independent authoritative provider value. Applied always retains a
    /// value, including JSON null; only `NotApplied` uses absence.
    pub actual_value: Option<Value>,
    /// Rust-derived canonical result Artifact for `actual_value`.
    pub result: Option<ArtifactRef>,
    /// Stable content identity of every preceding receipt field.
    pub receipt_id: String,
}

impl<'de> Deserialize<'de> for EffectResolutionReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            receipt_version: String,
            command: EffectResolutionCommand,
            actual_resolution: ReconciliationResolution,
            #[serde(deserialize_with = "Value::deserialize")]
            actual_value: Value,
            #[serde(deserialize_with = "deserialize_required_nullable")]
            result: Option<ArtifactRef>,
            receipt_id: String,
        }

        let wire = Wire::deserialize(deserializer)?;
        let actual_value = match wire.actual_resolution {
            ReconciliationResolution::ResolvedApplied => Some(wire.actual_value),
            ReconciliationResolution::ResolvedNotApplied if wire.actual_value.is_null() => None,
            _ => {
                return Err(serde::de::Error::custom(
                    "Effect receipt must retain an Applied value or a null NotApplied value",
                ));
            }
        };
        let receipt = Self {
            receipt_version: wire.receipt_version,
            command: wire.command,
            actual_resolution: wire.actual_resolution,
            actual_value,
            result: wire.result,
            receipt_id: wire.receipt_id,
        };
        receipt.verify_wire().map_err(serde::de::Error::custom)?;
        Ok(receipt)
    }
}

impl EffectResolutionReceipt {
    /// Validate the closed wire shape without deriving an Artifact identity.
    ///
    /// # Errors
    /// Returns an error for invalid version, command, identities, result kind, or
    /// inconsistent terminal outcome and required value/result presence.
    pub fn verify_wire(&self) -> DurableResult<()> {
        if self.receipt_version != EFFECT_RESOLUTION_RECEIPT_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported Effect resolution receipt version {}",
                self.receipt_version
            )));
        }
        self.command.verify()?;
        validate_receipt_id("Effect resolution", &self.receipt_id)?;
        if !is_terminal_resolution(self.actual_resolution) {
            return Err(DurableError::Validation(
                "Effect resolution receipt authority is malformed".to_owned(),
            ));
        }
        if let Some(result) = &self.result {
            result
                .validate()
                .map_err(|error| DurableError::Validation(error.to_string()))?;
            if result.kind != crate::model::EFFECT_RESULT_ARTIFACT_KIND {
                return Err(DurableError::Validation(
                    "Effect resolution result has the wrong Artifact kind".to_owned(),
                ));
            }
        }
        if self.actual_value.is_some() != self.result.is_some() {
            return Err(DurableError::Validation(
                "Effect resolution value and result presence disagree".to_owned(),
            ));
        }
        if self.actual_resolution == ReconciliationResolution::ResolvedApplied
            && self.actual_value.is_none()
        {
            return Err(DurableError::Validation(
                "Applied Effect receipt must retain its canonical result, including JSON null"
                    .to_owned(),
            ));
        }
        if self.actual_resolution == ReconciliationResolution::ResolvedNotApplied
            && (self.actual_value.is_some() || self.result.is_some())
        {
            return Err(DurableError::Validation(
                "NotApplied Effect receipt cannot carry a result".to_owned(),
            ));
        }
        Ok(())
    }

    /// Validate the authority-produced result identity in addition to wire shape.
    ///
    /// # Errors
    /// Returns an error for invalid wire semantics or a result or receipt identity
    /// that does not match its complete canonical preimage.
    pub fn verify(&self) -> DurableResult<()> {
        self.verify_wire()?;
        let expected = self
            .actual_value
            .as_ref()
            .map(|value| {
                cymule_core::artifact_ref(
                    crate::model::EFFECT_RESULT_ARTIFACT_KIND,
                    &cymule_core::canonical_bytes(value)?,
                )
            })
            .transpose()?;
        if self.result != expected || self.receipt_id != self.content_id()? {
            return Err(DurableError::Validation(
                "Effect resolution result or receipt identity is invalid".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn new(
        command: EffectResolutionCommand,
        actual_resolution: ReconciliationResolution,
        actual_value: Option<Value>,
        result: Option<ArtifactRef>,
    ) -> DurableResult<Self> {
        let mut receipt = Self {
            receipt_version: EFFECT_RESOLUTION_RECEIPT_VERSION.to_owned(),
            command,
            actual_resolution,
            actual_value,
            result,
            receipt_id: String::new(),
        };
        receipt.receipt_id = receipt.content_id()?;
        receipt.verify()?;
        Ok(receipt)
    }

    pub(crate) fn command_matches(&self, command: &EffectResolutionCommand) -> bool {
        // Required-nullable JSON has one wire value for None and Some(Null).
        // Canonical command bytes, not an in-memory Option distinction erased
        // by that wire, are the retained semantic replay authority.
        let Ok(retained) = cymule_core::canonical_bytes(&self.command) else {
            return false;
        };
        cymule_core::canonical_bytes(command).is_ok_and(|value| value == retained)
    }

    fn content_id(&self) -> DurableResult<String> {
        cymule_core::canonical_digest(&(
            self.receipt_version.as_str(),
            &self.command,
            self.actual_resolution,
            &self.actual_value,
            &self.result,
        ))
        .map_err(Into::into)
    }
}

/// Exact receipt for one semantic Run cancellation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancellationReceipt {
    /// Frozen receipt generation.
    pub receipt_version: String,
    /// Complete immutable cancellation command.
    pub command: CancellationCommand,
    /// Canonical terminal boundary bound to the same reason Artifact.
    pub boundary: DurableBoundary,
    /// Stable content identity of every preceding receipt field.
    pub receipt_id: String,
}

impl CancellationReceipt {
    /// Validate the closed wire shape without deriving an Artifact identity.
    ///
    /// # Errors
    /// Returns an error for invalid version, command, receipt identity, or
    /// cancellation boundary and reason Artifact kind.
    pub fn verify_wire(&self) -> DurableResult<()> {
        if self.receipt_version != RUN_CANCELLATION_RECEIPT_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported Run cancellation receipt version {}",
                self.receipt_version
            )));
        }
        self.command.verify()?;
        validate_receipt_id("Run cancellation", &self.receipt_id)?;
        let DurableBoundary::Cancelled { reason } = &self.boundary else {
            return Err(DurableError::Validation(
                "Run cancellation receipt reason and terminal boundary disagree".to_owned(),
            ));
        };
        reason
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if reason.kind != crate::coordinator::CANCELLATION_REASON_ARTIFACT_KIND {
            return Err(DurableError::Validation(
                "Run cancellation receipt reason and terminal boundary disagree".to_owned(),
            ));
        }
        self.boundary.verify()
    }

    /// Validate the authority-produced reason identity in addition to wire shape.
    ///
    /// # Errors
    /// Returns an error for invalid wire semantics or a reason or receipt identity
    /// that does not match the accepted cancellation command.
    pub fn verify(&self) -> DurableResult<()> {
        self.verify_wire()?;
        let expected = cymule_core::artifact_ref(
            crate::coordinator::CANCELLATION_REASON_ARTIFACT_KIND,
            &cymule_core::canonical_bytes(&self.command.reason)?,
        )?;
        if !matches!(&self.boundary, DurableBoundary::Cancelled { reason } if reason == &expected)
            || self.receipt_id != self.content_id()?
        {
            return Err(DurableError::Validation(
                "Run cancellation receipt reason or receipt identity is invalid".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn new(
        command: CancellationCommand,
        reason_ref: ArtifactRef,
    ) -> DurableResult<Self> {
        let mut receipt = Self {
            receipt_version: RUN_CANCELLATION_RECEIPT_VERSION.to_owned(),
            command,
            boundary: DurableBoundary::Cancelled { reason: reason_ref },
            receipt_id: String::new(),
        };
        receipt.receipt_id = receipt.content_id()?;
        receipt.verify()?;
        Ok(receipt)
    }

    pub(crate) fn command_matches(&self, command: &CancellationCommand) -> bool {
        &self.command == command
    }

    fn content_id(&self) -> DurableResult<String> {
        cymule_core::canonical_digest(&(
            self.receipt_version.as_str(),
            &self.command,
            &self.boundary,
        ))
        .map_err(Into::into)
    }
}

/// Closed response union for the M1 control protocol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableResponse {
    /// Run mutation reached a durable execution boundary.
    RunBoundary {
        /// Boundary receipt.
        boundary: DurableBoundary,
    },
    /// An identified wait delivery was admitted atomically.
    WaitActivated {
        /// Complete immutable selection, winner, and Ready-Run receipt.
        receipt: crate::WaitActivationReceipt,
    },
    /// One unknown-world Effect was terminally resolved without execution.
    EffectResolved {
        /// Complete binding, fence, value, and settlement receipt.
        receipt: EffectResolutionReceipt,
    },
    /// One Run cancellation was admitted or exactly replayed.
    RunCancelled {
        /// Complete original-reason and terminal-boundary receipt.
        receipt: CancellationReceipt,
    },
    /// One bounded page of the domain Run index.
    RunIndexPage {
        /// Revision/root-pinned Run summaries.
        page: DurableRunIndexPage,
    },
    /// One bounded semantic current projection.
    RunCurrent {
        /// Exact revision observed while resolving the Run.
        observed_revision: String,
        /// Canonical digest of the complete source `MapRoot`.
        source_root: String,
        /// Current projection, required on wire as null when the Run is absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        current: Option<Box<DurableRunCurrent>>,
    },
    /// One bounded page of a Run's waits.
    RunWaitPage {
        /// Owning Run.
        run_id: String,
        /// Revision/root-pinned wait summaries.
        page: DurableRunWaitPage,
    },
    /// One bounded page of a Run's Effects.
    RunEffectPage {
        /// Owning Run.
        run_id: String,
        /// Revision/root-pinned Effect summaries.
        page: DurableRunEffectPage,
    },
    /// One bounded page of a Run's component occurrences.
    RunOccurrencePage {
        /// Owning Run.
        run_id: String,
        /// Revision/root-pinned occurrence summaries.
        page: DurableRunOccurrencePage,
    },
    /// One bounded page of a Run's provider Attempts.
    RunAttemptPage {
        /// Owning Run.
        run_id: String,
        /// Revision/root-pinned Attempt summaries.
        page: DurableRunAttemptPage,
    },
    /// One complete Run-owned typed leaf resolved by exact identity.
    RunItem {
        /// Owning Run.
        run_id: String,
        /// Exact revision observed while resolving the item.
        observed_revision: String,
        /// Canonical digest of the complete source `MapRoot`.
        source_root: String,
        /// Exact item, required on wire as null when absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        item: Option<Box<DurableRunItem>>,
    },
}

impl DurableResponse {
    /// Validate a complete Engine wire response without re-deriving Artifact IDs.
    ///
    /// # Errors
    /// Returns an error for invalid nested wire semantics, query ownership or
    /// position evidence, or a response exceeding its canonical byte bound.
    pub fn verify_wire(&self) -> DurableResult<()> {
        match self {
            Self::RunBoundary { boundary } => boundary.verify(),
            Self::WaitActivated { receipt } => Ok(receipt.verify()?),
            Self::EffectResolved { receipt } => receipt.verify_wire(),
            Self::RunCancelled { receipt } => receipt.verify_wire(),
            Self::RunIndexPage { .. }
            | Self::RunWaitPage { .. }
            | Self::RunEffectPage { .. }
            | Self::RunOccurrencePage { .. }
            | Self::RunAttemptPage { .. } => self.verify_query_page_wire(),
            Self::RunCurrent {
                observed_revision,
                source_root,
                current,
            } => {
                verify_query_observation(observed_revision, source_root)?;
                if let Some(current) = current {
                    current.verify()?;
                }
                verify_canonical_size(
                    "durable Run-current response",
                    self,
                    query_byte_limit_usize(MAX_DURABLE_QUERY_PAGE_BYTES)?,
                )
            }
            Self::RunItem {
                run_id,
                observed_revision,
                source_root,
                item,
            } => verify_exact_run_item_response(
                self,
                run_id,
                observed_revision,
                source_root,
                item.as_deref(),
            ),
        }
    }

    fn verify_query_page_wire(&self) -> DurableResult<()> {
        match self {
            Self::RunIndexPage { page } => {
                page.verify(
                    DurablePageQueryKind::RunIndex,
                    None,
                    |item| item.run_id.as_str(),
                    |item, _| item.verify(),
                )?;
                verify_canonical_size(
                    "durable Run-index response",
                    self,
                    query_byte_limit_usize(MAX_DURABLE_QUERY_PAGE_BYTES)?,
                )
            }
            Self::RunWaitPage { run_id, page } => {
                verify_run_page(
                    run_id,
                    page,
                    DurablePageQueryKind::RunWaits,
                    |item| item.wait_id.as_str(),
                    DurableWaitSummary::verify,
                    |item| item.run_id.as_str(),
                )?;
                verify_canonical_size(
                    "durable Run-wait-page response",
                    self,
                    query_byte_limit_usize(MAX_DURABLE_QUERY_PAGE_BYTES)?,
                )
            }
            Self::RunEffectPage { run_id, page } => {
                verify_run_page(
                    run_id,
                    page,
                    DurablePageQueryKind::RunEffects,
                    |item| item.intent_id.as_str(),
                    DurableEffectSummary::verify,
                    |item| item.run_id.as_str(),
                )?;
                verify_canonical_size(
                    "durable Run-Effect-page response",
                    self,
                    query_byte_limit_usize(MAX_DURABLE_QUERY_PAGE_BYTES)?,
                )
            }
            Self::RunOccurrencePage { run_id, page } => {
                verify_run_page(
                    run_id,
                    page,
                    DurablePageQueryKind::RunOccurrences,
                    |item| item.occurrence_id.as_str(),
                    DurableOccurrenceSummary::verify,
                    |item| item.run_id.as_str(),
                )?;
                verify_canonical_size(
                    "durable Run-occurrence-page response",
                    self,
                    query_byte_limit_usize(MAX_DURABLE_QUERY_PAGE_BYTES)?,
                )
            }
            Self::RunAttemptPage { run_id, page } => {
                verify_run_page(
                    run_id,
                    page,
                    DurablePageQueryKind::RunAttempts,
                    |item| item.attempt_id.as_str(),
                    DurableAttemptSummary::verify,
                    |item| item.run_id.as_str(),
                )?;
                verify_canonical_size(
                    "durable Run-Attempt-page response",
                    self,
                    query_byte_limit_usize(MAX_DURABLE_QUERY_PAGE_BYTES)?,
                )
            }
            Self::RunBoundary { .. }
            | Self::WaitActivated { .. }
            | Self::EffectResolved { .. }
            | Self::RunCancelled { .. }
            | Self::RunCurrent { .. }
            | Self::RunItem { .. } => Err(DurableError::Validation(
                "non-page response reached the paged response verifier".to_owned(),
            )),
        }
    }

    /// Validate authority-produced content identities in addition to wire shape.
    ///
    /// # Errors
    /// Returns an error for invalid wire semantics or an authority-produced
    /// terminal receipt whose content identity is inconsistent.
    pub fn verify(&self) -> DurableResult<()> {
        self.verify_wire()?;
        match self {
            Self::EffectResolved { receipt } => receipt.verify(),
            Self::RunCancelled { receipt } => receipt.verify(),
            Self::RunBoundary { .. }
            | Self::WaitActivated { .. }
            | Self::RunIndexPage { .. }
            | Self::RunCurrent { .. }
            | Self::RunWaitPage { .. }
            | Self::RunEffectPage { .. }
            | Self::RunOccurrencePage { .. }
            | Self::RunAttemptPage { .. }
            | Self::RunItem { .. } => Ok(()),
        }
    }

    /// Validate one read-only response against the exact query that selected
    /// its revision, source root, owner, item count, and canonical byte budget.
    ///
    /// # Errors
    /// Returns an error if the query or response is invalid, or their variants,
    /// revision, root, owner, selector, cursor, or requested bounds disagree.
    pub fn verify_query_for(&self, command: &DurableCommand) -> DurableResult<()> {
        command.verify()?;
        self.verify_wire()?;
        match (command, self) {
            (
                DurableCommand::RunIndexPage {
                    expected_revision,
                    cursor,
                    limit,
                    max_canonical_bytes,
                    ..
                },
                Self::RunIndexPage { page },
            ) => verify_page_response_for(
                self,
                page,
                |item| item.run_id.as_str(),
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            (
                DurableCommand::RunCurrent {
                    run_id,
                    expected_revision,
                    ..
                },
                Self::RunCurrent {
                    observed_revision,
                    source_root,
                    current,
                },
            ) => {
                verify_query_result_binding(
                    expected_revision.as_deref(),
                    None,
                    observed_revision,
                    source_root,
                )?;
                if current
                    .as_ref()
                    .is_some_and(|current| current.run_id.as_str() != run_id.as_str())
                {
                    return Err(DurableError::Validation(
                        "Run-current response belongs to a different Run".to_owned(),
                    ));
                }
                Ok(())
            }
            (
                DurableCommand::RunWaitPage { .. }
                | DurableCommand::RunEffectPage { .. }
                | DurableCommand::RunOccurrencePage { .. }
                | DurableCommand::RunAttemptPage { .. },
                _,
            ) => self.verify_run_page_query_for(command),
            (
                DurableCommand::RunItem {
                    run_id,
                    expected_revision,
                    selector,
                    max_canonical_bytes,
                    ..
                },
                Self::RunItem {
                    run_id: response_run,
                    observed_revision,
                    source_root,
                    item,
                },
            ) if run_id == response_run => {
                verify_query_result_binding(
                    expected_revision.as_deref(),
                    None,
                    observed_revision,
                    source_root,
                )?;
                if item
                    .as_ref()
                    .is_some_and(|item| !run_item_matches_selector(item, selector))
                {
                    return Err(DurableError::Validation(
                        "exact durable Run-item response does not match its selector".to_owned(),
                    ));
                }
                verify_canonical_size(
                    "exact durable Run-item response",
                    self,
                    query_byte_limit_usize(*max_canonical_bytes)?,
                )
            }
            _ => Err(DurableError::Validation(
                "durable query command and response variants disagree".to_owned(),
            )),
        }
    }
    fn verify_run_page_query_for(&self, command: &DurableCommand) -> DurableResult<()> {
        match (command, self) {
            (
                DurableCommand::RunWaitPage {
                    run_id,
                    expected_revision,
                    cursor,
                    limit,
                    max_canonical_bytes,
                    ..
                },
                Self::RunWaitPage {
                    run_id: response_run,
                    page,
                },
            ) if run_id == response_run => verify_page_response_for(
                self,
                page,
                |item| item.wait_id.as_str(),
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            (
                DurableCommand::RunEffectPage {
                    run_id,
                    expected_revision,
                    cursor,
                    limit,
                    max_canonical_bytes,
                    ..
                },
                Self::RunEffectPage {
                    run_id: response_run,
                    page,
                },
            ) if run_id == response_run => verify_page_response_for(
                self,
                page,
                |item| item.intent_id.as_str(),
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            (
                DurableCommand::RunOccurrencePage {
                    run_id,
                    expected_revision,
                    cursor,
                    limit,
                    max_canonical_bytes,
                    ..
                },
                Self::RunOccurrencePage {
                    run_id: response_run,
                    page,
                },
            ) if run_id == response_run => verify_page_response_for(
                self,
                page,
                |item| item.occurrence_id.as_str(),
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            (
                DurableCommand::RunAttemptPage {
                    run_id,
                    expected_revision,
                    cursor,
                    limit,
                    max_canonical_bytes,
                    ..
                },
                Self::RunAttemptPage {
                    run_id: response_run,
                    page,
                },
            ) if run_id == response_run => verify_page_response_for(
                self,
                page,
                |item| item.attempt_id.as_str(),
                expected_revision.as_deref(),
                cursor.as_ref(),
                *limit,
                *max_canonical_bytes,
            ),
            _ => Err(DurableError::Validation(
                "durable query command and response variants disagree".to_owned(),
            )),
        }
    }
}

impl DurableBoundary {
    fn verify(&self) -> DurableResult<()> {
        match self {
            Self::Suspended { wait_id } => crate::model::validate_sha256_identity("wait", wait_id),
            Self::ReconciliationRequired { intent_id }
            | Self::EffectUnavailable { intent_id }
            | Self::EffectNotApplied { intent_id } => {
                crate::model::validate_sha256_identity("effect intent", intent_id)
            }
            Self::ReleaseRequired { intent_ids } => {
                if intent_ids.is_empty() {
                    return Err(DurableError::Validation(
                        "release-required boundary has no effect intents".to_owned(),
                    ));
                }
                for intent_id in intent_ids {
                    crate::model::validate_sha256_identity("effect intent", intent_id)?;
                }
                Ok(())
            }
            Self::Completed { result } => {
                validate_identity("Run", &result.run_id)?;
                crate::model::validate_sha256_identity("Plan", &result.plan_id)?;
                validate_raw_digest("execution projection", &result.projection_digest)?;
                validate_precondition_token(&result.precondition_token)?;
                cymule_core::canonical_bytes(&result.value)?;
                verify_sorted_unique(&result.effects, String::as_str, "effect intent")?;
                for intent_id in &result.effects {
                    crate::model::validate_sha256_identity("effect intent", intent_id)?;
                }
                Ok(())
            }
            Self::Failed { failure } => failure.verify().map_err(Into::into),
            Self::Cancelled { reason } => reason
                .validate()
                .map_err(|error| DurableError::Validation(error.to_string())),
        }
    }
}

fn verify_sorted_unique<T>(
    values: &[T],
    identity: impl Fn(&T) -> &str,
    kind: &str,
) -> DurableResult<()> {
    let mut previous = None;
    for value in values {
        let current = identity(value);
        validate_identity(kind, current)?;
        if previous.is_some_and(|previous| previous >= current) {
            return Err(DurableError::Validation(format!(
                "{kind} values are not strictly identity-ordered"
            )));
        }
        previous = Some(current);
    }
    Ok(())
}

/// Stateful Rust admission authority for one durable runtime.
pub struct DurableRuntimeControl<S, P> {
    runtime: ResumableRuntime<S, P>,
}

impl<S: DurableStore, P: BoundPluginHost> DurableRuntimeControl<S, P> {
    /// Open one resumable runtime over an exact provider admission and Clock.
    ///
    /// # Errors
    /// Returns an error if the retained Store or admitted execution-binding
    /// authority cannot be authenticated.
    pub fn open<C: crate::ExecutionClockAuthority + 'static>(
        store: S,
        admission: ExecutionBindingAdmission<P>,
        clock: C,
    ) -> DurableResult<Self> {
        Ok(Self {
            runtime: ResumableRuntime::open(store, admission, clock)?,
        })
    }

    /// Submit one verified command to the Rust authority.
    ///
    /// # Errors
    /// Returns an error for invalid or stale command, binding, or execution
    /// authority, failed execution, or failed or uncertain Store publication.
    pub fn submit(&mut self, command: DurableCommand) -> DurableResult<DurableResponse> {
        command.verify()?;
        if command.is_read_only() {
            return self.runtime.coordinator_mut().query(&command);
        }
        self.submit_mutation(command)
    }

    fn submit_mutation(&mut self, command: DurableCommand) -> DurableResult<DurableResponse> {
        match command {
            DurableCommand::StartRun {
                run_id,
                candidate,
                input,
                execution,
                ..
            } => Ok(DurableResponse::RunBoundary {
                boundary: self
                    .runtime
                    .start(candidate, &input, run_id, &execution)?
                    .into(),
            }),
            DurableCommand::ResumeRun {
                run_id, execution, ..
            } => Ok(DurableResponse::RunBoundary {
                boundary: self.runtime.resume(&run_id, &execution)?.into(),
            }),
            DurableCommand::TakeoverRun {
                run_id,
                expected_fence,
                execution,
                ..
            } => Ok(DurableResponse::RunBoundary {
                boundary: self
                    .runtime
                    .takeover(&run_id, expected_fence, &execution)?
                    .into(),
            }),
            DurableCommand::ActivateWait {
                activation_id,
                source,
                wait_ids,
                value,
                ..
            } => {
                let receipt = self
                    .runtime
                    .coordinator_mut()
                    .admit_wait_activation_receipt(activation_id, source, wait_ids, &value)?;
                Ok(DurableResponse::WaitActivated { receipt })
            }
            DurableCommand::ReleaseEffect {
                intent_id,
                execution,
                ..
            } => Ok(DurableResponse::RunBoundary {
                boundary: self.runtime.release_effect(&intent_id, &execution)?.into(),
            }),
            DurableCommand::ResolveEffect {
                resolution_id,
                run_id,
                intent_id,
                execution_binding,
                occurrence_binding,
                claim_owner,
                claim_epoch,
                resolution,
                value,
                ..
            } => {
                let receipt =
                    self.runtime
                        .resolve_effect_with_provider(&EffectResolutionCommand {
                            resolution_id,
                            run_id,
                            intent_id,
                            execution_binding,
                            occurrence_binding,
                            claim_owner,
                            claim_epoch,
                            resolution,
                            value,
                        })?;
                Ok(DurableResponse::EffectResolved { receipt })
            }
            DurableCommand::CancelRun {
                cancellation_id,
                run_id,
                reason,
                ..
            } => {
                let receipt = self.runtime.coordinator_mut().cancel_run(
                    &run_id,
                    &cancellation_id,
                    &reason,
                )?;
                Ok(DurableResponse::RunCancelled { receipt })
            }
            DurableCommand::RunIndexPage { .. }
            | DurableCommand::RunCurrent { .. }
            | DurableCommand::RunWaitPage { .. }
            | DurableCommand::RunEffectPage { .. }
            | DurableCommand::RunOccurrencePage { .. }
            | DurableCommand::RunAttemptPage { .. }
            | DurableCommand::RunItem { .. } => Err(DurableError::RuntimeDefect {
                code: "query_bypassed_control_dispatch".to_owned(),
                message: "a verified query bypassed the closed query dispatch".to_owned(),
            }),
        }
    }

    /// Borrow the closed Resource-profile persistence authority.
    pub fn resource(&mut self) -> DurableResourceControl<'_, S> {
        DurableResourceControl {
            coordinator: self.runtime.coordinator_mut(),
        }
    }

    /// Borrow the closed Agent-profile persistence authority together with its
    /// exact provider registry.
    pub fn agent<'a>(
        &'a mut self,
        providers: &'a mut dyn agent_protocol::AgentProviders,
    ) -> DurableAgentControl<'a, S> {
        let (coordinator, clock) = self.runtime.profile_authorities();
        DurableAgentControl {
            coordinator,
            providers,
            clock,
        }
    }

    /// Borrow the closed M3 authority with its exact provider registry.
    pub fn virtual_work<'a>(
        &'a mut self,
        providers: &'a mut dyn virtual_protocol::VirtualProviders,
    ) -> DurableVirtualControl<'a, S> {
        let execution_binding = self.runtime.execution_binding().clone();
        let (coordinator, clock) = self.runtime.profile_authorities();
        DurableVirtualControl {
            coordinator,
            providers,
            clock,
            execution_binding,
        }
    }

    /// Drive one bounded replaceable wait source through receive, exact
    /// activation admission, and post-CAS acknowledgement. A lost
    /// acknowledgement redelivers the same activation and never creates a
    /// second semantic transition.
    ///
    /// # Errors
    /// Returns an error for invalid delivery bounds, unavailable or invalid source
    /// evidence, failed activation publication, or acknowledgement failure.
    pub fn drive_wait_source<D: crate::WaitSourceDriver>(
        &mut self,
        driver: &mut D,
        max_targets: usize,
    ) -> DurableResult<Option<crate::WaitAdmissionOutcome>> {
        self.runtime.drive_wait_source(driver, max_targets)
    }

    /// Consume the controller into its Store and admitted provider.
    pub fn into_parts(self) -> (S, P) {
        self.runtime.into_parts()
    }
}

fn verify_exact_run_item_response(
    response: &DurableResponse,
    run_id: &str,
    observed_revision: &str,
    source_root: &str,
    item: Option<&DurableRunItem>,
) -> DurableResult<()> {
    validate_identity("Run", run_id)?;
    verify_query_observation(observed_revision, source_root)?;
    if let Some(item) = item {
        item.verify()?;
        if item.run_id() != run_id {
            return Err(DurableError::Validation(
                "exact durable Run item belongs to a different Run".to_owned(),
            ));
        }
    }
    verify_canonical_size(
        "exact durable Run-item response",
        response,
        query_byte_limit_usize(MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES)?,
    )
}

fn verify_wait_activation_command(
    activation_id: &str,
    source: &WaitActivationSource,
    wait_ids: &BTreeSet<String>,
) -> DurableResult<()> {
    validate_identity("activation", activation_id)?;
    source.verify()?;
    if wait_ids.is_empty() || wait_ids.len() > crate::MAX_WAIT_DELIVERY_TARGETS {
        return Err(DurableError::Validation(format!(
            "wait activation target count must be 1..={}",
            crate::MAX_WAIT_DELIVERY_TARGETS
        )));
    }
    for wait_id in wait_ids {
        crate::model::validate_sha256_identity("wait", wait_id)?;
    }
    Ok(())
}

fn verify_run_item_query(
    run_id: &str,
    expected_revision: Option<&str>,
    selector: &DurableRunItemSelector,
    max_canonical_bytes: u64,
) -> DurableResult<()> {
    verify_exact_query(run_id, expected_revision)?;
    selector.verify()?;
    verify_query_byte_budget(
        "exact Run-item query",
        max_canonical_bytes,
        MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
    )
}

fn verify_page_query(
    query_kind: DurablePageQueryKind,
    run_id: Option<&str>,
    expected_revision: Option<&str>,
    cursor: Option<&DurablePageCursor>,
    limit: u32,
    max_canonical_bytes: u64,
) -> DurableResult<()> {
    if query_kind.is_run_scoped() != run_id.is_some() {
        return Err(DurableError::Validation(
            "durable page query owner does not match its query kind".to_owned(),
        ));
    }
    if let Some(run_id) = run_id {
        validate_identity("Run", run_id)?;
    }
    verify_expected_revision(expected_revision)?;
    if !(1..=MAX_DURABLE_QUERY_PAGE_ITEMS).contains(&limit) {
        return Err(DurableError::Validation(format!(
            "durable query page limit must be within 1..={MAX_DURABLE_QUERY_PAGE_ITEMS}"
        )));
    }
    verify_query_byte_budget(
        "durable page query",
        max_canonical_bytes,
        MAX_DURABLE_QUERY_PAGE_BYTES,
    )?;
    if let Some(cursor) = cursor {
        cursor.verify_scope(query_kind, run_id)?;
        if expected_revision != Some(cursor.source_revision.as_str()) {
            return Err(DurableError::Conflict {
                expected: expected_revision.map(str::to_owned),
                current: Some(cursor.source_revision.clone()),
            });
        }
    }
    Ok(())
}

fn verify_exact_query(run_id: &str, expected_revision: Option<&str>) -> DurableResult<()> {
    validate_identity("Run", run_id)?;
    verify_expected_revision(expected_revision)
}

fn verify_expected_revision(expected_revision: Option<&str>) -> DurableResult<()> {
    if let Some(expected_revision) = expected_revision {
        cymule_core::validate_content_id("durable query expected revision", expected_revision)?;
    }
    Ok(())
}

fn verify_query_byte_budget(kind: &str, value: u64, maximum: u64) -> DurableResult<()> {
    if value == 0 || value > maximum || value > crate::MAX_EXACT_INTEGER {
        return Err(DurableError::Validation(format!(
            "{kind} canonical byte budget must be within 1..={maximum}"
        )));
    }
    Ok(())
}

fn query_byte_limit_usize(value: u64) -> DurableResult<usize> {
    usize::try_from(value).map_err(|_| {
        DurableError::Validation(
            "durable query canonical byte bound is not representable on this target".to_owned(),
        )
    })
}

fn durable_page_key_hash(canonical_key: &str) -> DurableResult<String> {
    cymule_authenticated_collections::map_key_hash(canonical_key).map_err(Into::into)
}

fn verify_canonical_size<T: Serialize>(kind: &str, value: &T, maximum: usize) -> DurableResult<()> {
    let bytes = cymule_core::canonical_bytes(value)?;
    if bytes.len() > maximum {
        return Err(DurableError::Validation(format!(
            "{kind} exceeds {maximum} canonical bytes"
        )));
    }
    Ok(())
}

fn verify_summary_size<T: Serialize>(kind: &str, value: &T) -> DurableResult<()> {
    verify_canonical_size(kind, value, MAX_DURABLE_QUERY_SUMMARY_BYTES)
}

fn verify_query_observation(observed_revision: &str, source_root: &str) -> DurableResult<()> {
    cymule_core::validate_content_id("durable query observed revision", observed_revision)?;
    validate_raw_digest("durable query source root", source_root)
}

fn verify_page_response_for<T: Serialize>(
    response: &DurableResponse,
    page: &DurableQueryPage<T>,
    item_key: impl for<'a> Fn(&'a T) -> &'a str,
    expected_revision: Option<&str>,
    cursor: Option<&DurablePageCursor>,
    limit: u32,
    max_canonical_bytes: u64,
) -> DurableResult<()> {
    verify_query_result_binding(
        expected_revision,
        cursor,
        &page.observed_revision,
        &page.source_root,
    )?;
    if let (Some(cursor), Some(first)) = (cursor, page.items.first()) {
        let first = DurablePagePosition::for_key(item_key(first))?;
        if (first.key_hash.as_str(), first.canonical_key.as_str())
            <= (
                cursor.position.key_hash.as_str(),
                cursor.position.canonical_key.as_str(),
            )
        {
            return Err(DurableError::Validation(
                "durable query continuation did not advance past its cursor".to_owned(),
            ));
        }
    }
    if u32::try_from(page.items.len()).map_or(true, |count| count > limit) {
        return Err(DurableError::Validation(
            "durable query response exceeds its requested item limit".to_owned(),
        ));
    }
    verify_canonical_size(
        "durable query response",
        response,
        query_byte_limit_usize(max_canonical_bytes)?,
    )
}

fn verify_query_result_binding(
    expected_revision: Option<&str>,
    cursor: Option<&DurablePageCursor>,
    observed_revision: &str,
    source_root: &str,
) -> DurableResult<()> {
    verify_query_observation(observed_revision, source_root)?;
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
    } else if let Some(expected_revision) = expected_revision
        && expected_revision != observed_revision
    {
        return Err(DurableError::Conflict {
            expected: Some(expected_revision.to_owned()),
            current: Some(observed_revision.to_owned()),
        });
    }
    Ok(())
}

fn run_item_matches_selector(item: &DurableRunItem, selector: &DurableRunItemSelector) -> bool {
    match (item, selector) {
        (DurableRunItem::Wait { wait }, DurableRunItemSelector::Wait { wait_id }) => {
            wait.wait_id.as_str() == wait_id.as_str()
        }
        (DurableRunItem::Effect { effect }, DurableRunItemSelector::Effect { intent_id }) => {
            effect.intent_id.as_str() == intent_id.as_str()
        }
        (
            DurableRunItem::Occurrence { occurrence },
            DurableRunItemSelector::Occurrence { occurrence_id },
        ) => occurrence.occurrence_id.as_str() == occurrence_id.as_str(),
        (DurableRunItem::Attempt { attempt }, DurableRunItemSelector::Attempt { attempt_id }) => {
            attempt.attempt_id.as_str() == attempt_id.as_str()
        }
        _ => false,
    }
}

fn verify_continuation_execution_axes(
    continuation_status: ContinuationStatus,
    execution_status: &RunExecutionStatus,
) -> DurableResult<()> {
    if matches!(
        (continuation_status, execution_status),
        (
            ContinuationStatus::Ready | ContinuationStatus::Waiting | ContinuationStatus::Running,
            RunExecutionStatus::Active,
        ) | (ContinuationStatus::Completed, RunExecutionStatus::Completed)
            | (
                ContinuationStatus::Failed,
                RunExecutionStatus::Failed { .. }
            )
            | (
                ContinuationStatus::Cancelled,
                RunExecutionStatus::Cancelled { .. }
            )
    ) {
        Ok(())
    } else {
        Err(DurableError::Validation(
            "Run continuation and execution axes disagree".to_owned(),
        ))
    }
}

fn verify_run_axes(
    execution_status: &RunExecutionStatus,
    world_settlement: WorldSettlementStatus,
    result: Option<&ArtifactRef>,
    require_result_projection: bool,
) -> DurableResult<()> {
    match execution_status {
        RunExecutionStatus::Failed { failure } => failure.verify()?,
        RunExecutionStatus::Cancelled { reason } => reason
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?,
        RunExecutionStatus::Active | RunExecutionStatus::Completed => {}
    }
    if require_result_projection {
        match (execution_status, result) {
            (RunExecutionStatus::Completed, Some(result)) => result
                .validate()
                .map_err(|error| DurableError::Validation(error.to_string()))?,
            (RunExecutionStatus::Completed, None) => {
                return Err(DurableError::Validation(
                    "completed Run current requires its terminal result".to_owned(),
                ));
            }
            (_, None) => {}
            (_, Some(_)) => {
                return Err(DurableError::Validation(
                    "only a completed Run current may carry a terminal result".to_owned(),
                ));
            }
        }
    }
    if matches!(execution_status, RunExecutionStatus::Completed)
        && world_settlement != WorldSettlementStatus::Settled
    {
        return Err(DurableError::Validation(
            "completed Run projection retains unsettled external-world Effects".to_owned(),
        ));
    }
    Ok(())
}

fn verify_run_page<T: Serialize>(
    run_id: &str,
    page: &DurableQueryPage<T>,
    query_kind: DurablePageQueryKind,
    item_key: impl for<'a> Fn(&'a T) -> &'a str,
    verify_item: impl Fn(&T) -> DurableResult<()>,
    item_run_id: impl for<'a> Fn(&'a T) -> &'a str,
) -> DurableResult<()> {
    validate_identity("Run", run_id)?;
    page.verify(query_kind, Some(run_id), item_key, |item, expected_run| {
        verify_item(item)?;
        if Some(item_run_id(item)) != expected_run {
            return Err(DurableError::Validation(
                "durable query page contains an item owned by a different Run".to_owned(),
            ));
        }
        Ok(())
    })
}

fn validate_identity(kind: &str, value: &str) -> DurableResult<()> {
    crate::model::validate_wire_non_empty(&format!("durable {kind} identity"), value)
}

fn validate_receipt_id(kind: &str, value: &str) -> DurableResult<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(DurableError::Validation(format!(
            "{kind} receipt_id must be an exact lowercase SHA-256 digest"
        )))
    }
}

fn validate_raw_digest(kind: &str, value: &str) -> DurableResult<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(DurableError::Validation(format!(
            "{kind} must be an exact lowercase SHA-256 digest"
        )))
    }
}

fn validate_precondition_token(value: &str) -> DurableResult<()> {
    let Some(rest) = value.strip_prefix("pre:") else {
        return Err(DurableError::Validation(
            "execution precondition token is malformed".to_owned(),
        ));
    };
    let Some((epoch, last_event)) = rest.split_once(':') else {
        return Err(DurableError::Validation(
            "execution precondition token is malformed".to_owned(),
        ));
    };
    let parsed_epoch = epoch.parse::<u64>().map_err(|_| {
        DurableError::Validation("execution precondition token epoch is malformed".to_owned())
    })?;
    if parsed_epoch > crate::MAX_EXACT_INTEGER || parsed_epoch.to_string() != epoch {
        return Err(DurableError::Validation(
            "execution precondition token epoch is outside the canonical exact range".to_owned(),
        ));
    }
    crate::model::validate_sha256_identity("execution precondition Event", last_event)
}

const fn is_terminal_resolution(resolution: ReconciliationResolution) -> bool {
    matches!(
        resolution,
        ReconciliationResolution::ResolvedApplied | ReconciliationResolution::ResolvedNotApplied
    )
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod query_protocol_tests {
    use super::*;

    fn content_id(label: &str) -> String {
        cymule_core::content_id("test.durable-query/1", &label).expect("content ID derives")
    }

    fn root_digest(label: &str) -> String {
        cymule_core::canonical_digest(&("test.durable-query-root/1", label))
            .expect("root digest derives")
    }

    fn execution_binding(label: &str) -> ArtifactRef {
        ArtifactRef {
            identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
            artifact_id: content_id(label),
            kind: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
        }
    }

    fn resolution_command() -> EffectResolutionCommand {
        EffectResolutionCommand {
            resolution_id: "resolution:null-result".to_owned(),
            run_id: "run:null-result".to_owned(),
            intent_id: content_id("intent:null-result"),
            execution_binding: execution_binding("null-result"),
            occurrence_binding: content_id("occurrence:null-result"),
            claim_owner: "driver:null-result".to_owned(),
            claim_epoch: 1,
            resolution: ReconciliationResolution::ResolvedApplied,
            value: None,
        }
    }

    #[test]
    fn applied_null_resolution_receipt_roundtrips_without_losing_its_result() {
        let result = cymule_core::artifact_ref(crate::model::EFFECT_RESULT_ARTIFACT_KIND, b"null")
            .expect("null result Artifact seals");
        let receipt = EffectResolutionReceipt::new(
            resolution_command(),
            ReconciliationResolution::ResolvedApplied,
            Some(Value::Null),
            Some(result),
        )
        .expect("Applied null receipt seals");
        let bytes = cymule_core::canonical_bytes(&receipt).expect("receipt encodes");
        let reopened: EffectResolutionReceipt =
            cymule_core::decode_json(&bytes).expect("receipt reopens through strict JSON");
        assert_eq!(reopened, receipt);
        reopened
            .verify()
            .expect("reopened complete receipt verifies");

        for field in ["actual_value", "result"] {
            let mut missing = serde_json::to_value(&receipt).expect("receipt encodes");
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<EffectResolutionReceipt>(missing).is_err());
        }
        assert!(
            EffectResolutionReceipt::new(
                resolution_command(),
                ReconciliationResolution::ResolvedApplied,
                None,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn not_applied_resolution_receipt_retains_only_explicit_absence() {
        let receipt = EffectResolutionReceipt::new(
            resolution_command(),
            ReconciliationResolution::ResolvedNotApplied,
            None,
            None,
        )
        .expect("NotApplied absence seals");
        let value = serde_json::to_value(&receipt).expect("receipt encodes");
        assert_eq!(value["actual_value"], Value::Null);
        assert_eq!(value["result"], Value::Null);
        let reopened: EffectResolutionReceipt =
            serde_json::from_value(value.clone()).expect("NotApplied explicit absence reopens");
        assert_eq!(reopened, receipt);
        let mut invalid = value;
        invalid["actual_value"] = serde_json::json!({"unexpected": true});
        assert!(serde_json::from_value::<EffectResolutionReceipt>(invalid).is_err());
    }

    #[test]
    fn resolution_replay_matches_required_null_command_semantics_after_reopen() {
        let mut command = resolution_command();
        command.value = Some(Value::Null);
        let result = cymule_core::artifact_ref(crate::model::EFFECT_RESULT_ARTIFACT_KIND, b"null")
            .expect("null result Artifact seals");
        let receipt = EffectResolutionReceipt::new(
            command.clone(),
            ReconciliationResolution::ResolvedApplied,
            Some(Value::Null),
            Some(result),
        )
        .expect("receipt seals");
        let reopened: EffectResolutionReceipt = cymule_core::decode_json(
            &cymule_core::canonical_bytes(&receipt).expect("receipt encodes"),
        )
        .expect("receipt reopens");
        assert_eq!(reopened.command.value, None);
        reopened.verify().expect("canonical receipt remains exact");
        assert!(reopened.command_matches(&command));
        command.value = Some(Value::String("different".to_owned()));
        assert!(!reopened.command_matches(&command));
    }

    fn cursor(kind: DurablePageQueryKind, run_id: Option<&str>, key: &str) -> DurablePageCursor {
        DurablePageCursor {
            query_kind: kind,
            run_id: run_id.map(str::to_owned),
            source_revision: content_id("revision"),
            source_root: root_digest("root"),
            position: DurablePagePosition::for_key(key).expect("position derives"),
        }
    }

    fn pending_wait(run_id: &str) -> WaitCondition {
        WaitCondition {
            wait_id: content_id("wait"),
            run_id: run_id.to_owned(),
            kind: crate::WaitKind::Signal {
                key: "signal-key".to_owned(),
            },
            consume_once: true,
            owner: crate::WaitOwner {
                invocation_id: "invocation".to_owned(),
                definition_id: "definition".to_owned(),
                region_path: Vec::new(),
                site_id: "site".to_owned(),
                step_index: 0,
                bind: None,
            },
            state: WaitState::Pending,
            result: None,
        }
    }

    fn assert_required_nullable_fields<T>(value: &T, required: &[&str])
    where
        T: Serialize + serde::de::DeserializeOwned,
    {
        let Value::Object(fields) = serde_json::to_value(value).expect("value serializes") else {
            panic!("required-nullable value must serialize as an object");
        };
        for field in required {
            assert_eq!(fields.get(*field), Some(&Value::Null));
            serde_json::from_value::<T>(Value::Object(fields.clone()))
                .expect("explicit null decodes");
            let mut missing = fields.clone();
            missing.remove(*field);
            assert!(
                serde_json::from_value::<T>(Value::Object(missing)).is_err(),
                "missing required-nullable field {field} must fail closed"
            );
        }
    }

    #[test]
    fn query_optional_members_are_required_nullable_on_wire() {
        assert_query_command_nullable_fields();
        assert_query_result_nullable_fields();
    }

    fn assert_query_command_nullable_fields() {
        let command = DurableCommand::RunIndexPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            expected_revision: None,
            cursor: None,
            limit: 1,
            max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
        };
        assert_required_nullable_fields(&command, &["expected_revision", "cursor"]);

        for command in [
            DurableCommand::RunWaitPage {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run".to_owned(),
                expected_revision: None,
                cursor: None,
                limit: 1,
                max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
            },
            DurableCommand::RunEffectPage {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run".to_owned(),
                expected_revision: None,
                cursor: None,
                limit: 1,
                max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
            },
            DurableCommand::RunOccurrencePage {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run".to_owned(),
                expected_revision: None,
                cursor: None,
                limit: 1,
                max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
            },
            DurableCommand::RunAttemptPage {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run".to_owned(),
                expected_revision: None,
                cursor: None,
                limit: 1,
                max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
            },
        ] {
            assert_required_nullable_fields(&command, &["expected_revision", "cursor"]);
        }
        assert_required_nullable_fields(
            &DurableCommand::RunCurrent {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run".to_owned(),
                expected_revision: None,
            },
            &["expected_revision"],
        );
        assert_required_nullable_fields(
            &DurableCommand::RunItem {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run".to_owned(),
                expected_revision: None,
                selector: DurableRunItemSelector::Wait {
                    wait_id: content_id("wait"),
                },
                max_canonical_bytes: MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
            },
            &["expected_revision"],
        );
    }

    fn assert_query_result_nullable_fields() {
        let cursor = cursor(DurablePageQueryKind::RunIndex, None, "run");
        assert_required_nullable_fields(&cursor, &["run_id"]);

        let page = DurableRunIndexPage {
            observed_revision: content_id("revision"),
            source_root: root_digest("root"),
            items: Vec::new(),
            next_cursor: None,
        };
        assert_required_nullable_fields(&page, &["next_cursor"]);

        let current = DurableRunCurrent {
            run_id: "run".to_owned(),
            plan_id: content_id("plan"),
            execution_binding: execution_binding("binding"),
            continuation_status: ContinuationStatus::Ready,
            epoch: 0,
            execution_fence: 0,
            result: None,
            execution_status: RunExecutionStatus::Active,
            world_settlement: WorldSettlementStatus::Settled,
        };
        assert_required_nullable_fields(&current, &["result"]);
        assert_required_nullable_fields(
            &DurableWaitSummary {
                wait_id: content_id("wait"),
                run_id: "run".to_owned(),
                state: WaitState::Pending,
                result: None,
            },
            &["result"],
        );
        assert_required_nullable_fields(
            &DurableEffectSummary {
                intent_id: content_id("effect"),
                run_id: "run".to_owned(),
                state: OutboxState::Pending,
                execution_availability: cymule_core::EffectExecutionAvailability::Available,
                reconciliation: cymule_core::ReconciliationState::NotRequired,
                result: None,
            },
            &["result"],
        );
        assert_required_nullable_fields(
            &DurableOccurrenceSummary {
                occurrence_id: content_id("occurrence"),
                run_id: "run".to_owned(),
                state: ComponentOccurrenceState::Pending,
                outcome: None,
            },
            &["outcome"],
        );
        assert_required_nullable_fields(
            &DurableAttemptSummary {
                attempt_id: content_id("attempt"),
                occurrence_id: content_id("occurrence"),
                run_id: "run".to_owned(),
                attempt_ordinal: 1,
                state: OperationAttemptState::Running,
                outcome: None,
            },
            &["outcome"],
        );
        assert_required_nullable_fields(
            &DurableResponse::RunCurrent {
                observed_revision: content_id("revision"),
                source_root: root_digest("current"),
                current: None,
            },
            &["current"],
        );
        assert_required_nullable_fields(
            &DurableResponse::RunItem {
                run_id: "run".to_owned(),
                observed_revision: content_id("revision"),
                source_root: root_digest("item"),
                item: None,
            },
            &["item"],
        );
    }

    #[test]
    fn control_four_hard_rejects_old_and_unknown_query_shapes() {
        let old = serde_json::json!({
            "type": "query_run",
            "control_version": "cymule.durable-control/3",
            "run_id": "run"
        });
        assert!(serde_json::from_value::<DurableCommand>(old).is_err());

        let wrong_version = DurableCommand::RunCurrent {
            control_version: "cymule.durable-control/3".to_owned(),
            run_id: "run".to_owned(),
            expected_revision: None,
        };
        assert!(wrong_version.verify().is_err());

        let mut unknown = serde_json::to_value(DurableCommand::RunCurrent {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run".to_owned(),
            expected_revision: None,
        })
        .expect("command serializes");
        unknown
            .as_object_mut()
            .expect("command is an object")
            .insert("legacy_view".to_owned(), Value::Bool(true));
        assert!(serde_json::from_value::<DurableCommand>(unknown).is_err());

        let negative_limit = serde_json::json!({
            "type": "run_index_page",
            "control_version": DURABLE_CONTROL_VERSION,
            "expected_revision": null,
            "cursor": null,
            "limit": -1,
            "max_canonical_bytes": MAX_DURABLE_QUERY_PAGE_BYTES,
        });
        assert!(serde_json::from_value::<DurableCommand>(negative_limit).is_err());
    }

    #[test]
    fn cursor_requires_the_exact_revision_query_owner_root_and_full_position() {
        let cursor = cursor(
            DurablePageQueryKind::RunWaits,
            Some("run"),
            &content_id("wait"),
        );
        let command = |expected_revision: Option<String>, cursor: DurablePageCursor| {
            DurableCommand::RunWaitPage {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run".to_owned(),
                expected_revision,
                cursor: Some(cursor),
                limit: 16,
                max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
            }
        };
        assert!(matches!(
            command(None, cursor.clone()).verify(),
            Err(DurableError::Conflict {
                expected: None,
                current: Some(_)
            })
        ));
        assert!(matches!(
            command(Some(content_id("other revision")), cursor.clone()).verify(),
            Err(DurableError::Conflict {
                expected: Some(_),
                current: Some(_)
            })
        ));
        command(Some(cursor.source_revision.clone()), cursor.clone())
            .verify()
            .expect("exact cursor revision verifies");

        let mut wrong_kind = cursor.clone();
        wrong_kind.query_kind = DurablePageQueryKind::RunEffects;
        assert!(
            command(Some(cursor.source_revision.clone()), wrong_kind)
                .verify()
                .is_err()
        );

        let mut wrong_run = cursor.clone();
        wrong_run.run_id = Some("other-run".to_owned());
        assert!(
            command(Some(cursor.source_revision.clone()), wrong_run)
                .verify()
                .is_err()
        );

        let mut wrong_hash = cursor;
        wrong_hash.position.key_hash = root_digest("wrong hash");
        assert!(
            command(Some(wrong_hash.source_revision.clone()), wrong_hash)
                .verify()
                .is_err()
        );

        let invalid_wait_key = DurablePageCursor {
            query_kind: DurablePageQueryKind::RunWaits,
            run_id: Some("run".to_owned()),
            source_revision: content_id("revision"),
            source_root: root_digest("root"),
            position: DurablePagePosition::for_key("not-a-content-id")
                .expect("generic position derives"),
        };
        assert!(invalid_wait_key.verify().is_err());

        let oversized_key = "x".repeat(MAX_DURABLE_QUERY_RUN_KEY_SCALARS + 1);
        let oversized_cursor = DurablePageCursor {
            query_kind: DurablePageQueryKind::RunIndex,
            run_id: None,
            source_revision: content_id("revision"),
            source_root: root_digest("root"),
            position: DurablePagePosition {
                key_hash: durable_page_key_hash(&oversized_key).expect("hash derives"),
                canonical_key: oversized_key,
            },
        };
        assert!(oversized_cursor.verify().is_err());
    }

    #[test]
    fn page_query_limits_budgets_and_exact_integers_fail_closed() {
        let command = |limit, max_canonical_bytes| DurableCommand::RunIndexPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            expected_revision: None,
            cursor: None,
            limit,
            max_canonical_bytes,
        };
        assert!(command(1, 1).verify().is_ok());
        assert!(command(0, MAX_DURABLE_QUERY_PAGE_BYTES).verify().is_err());
        assert!(command(257, MAX_DURABLE_QUERY_PAGE_BYTES).verify().is_err());
        assert!(command(1, 0).verify().is_err());
        assert!(
            command(1, MAX_DURABLE_QUERY_PAGE_BYTES + 1)
                .verify()
                .is_err()
        );

        let mut current = DurableRunCurrent {
            run_id: "run".to_owned(),
            plan_id: content_id("plan"),
            execution_binding: execution_binding("binding"),
            continuation_status: ContinuationStatus::Ready,
            epoch: crate::MAX_EXACT_INTEGER,
            execution_fence: crate::MAX_EXACT_INTEGER,
            result: None,
            execution_status: RunExecutionStatus::Active,
            world_settlement: WorldSettlementStatus::Settled,
        };
        current.verify().expect("maximum exact integers verify");
        current.epoch = crate::MAX_EXACT_INTEGER + 1;
        assert!(current.verify().is_err());
    }

    #[test]
    fn page_order_and_next_cursor_are_bound_to_the_authenticated_hash_order() {
        let mut items = ["run-a", "run-b"]
            .into_iter()
            .map(|run_id| DurableRunIndexSummary {
                run_id: run_id.to_owned(),
                continuation_status: ContinuationStatus::Ready,
                execution_status: RunExecutionStatus::Active,
                world_settlement: WorldSettlementStatus::Settled,
            })
            .collect::<Vec<_>>();
        items.sort_by_key(|item| {
            let position = DurablePagePosition::for_key(&item.run_id).expect("position derives");
            (position.key_hash, position.canonical_key)
        });
        let last = items.last().expect("two items").run_id.clone();
        let page = DurableRunIndexPage {
            observed_revision: content_id("revision"),
            source_root: root_digest("root"),
            items,
            next_cursor: Some(cursor(DurablePageQueryKind::RunIndex, None, &last)),
        };
        DurableResponse::RunIndexPage { page: page.clone() }
            .verify_wire()
            .expect("hash-ordered page verifies");

        let mut reversed = page.clone();
        reversed.items.reverse();
        assert!(
            DurableResponse::RunIndexPage { page: reversed }
                .verify_wire()
                .is_err()
        );

        let mut wrong_root = page;
        wrong_root
            .next_cursor
            .as_mut()
            .expect("cursor exists")
            .source_root = root_digest("other root");
        assert!(
            DurableResponse::RunIndexPage { page: wrong_root }
                .verify_wire()
                .is_err()
        );
    }

    #[test]
    fn response_is_bound_to_the_requested_revision_root_limit_and_budget() {
        let item = DurableRunIndexSummary {
            run_id: "run".to_owned(),
            continuation_status: ContinuationStatus::Ready,
            execution_status: RunExecutionStatus::Active,
            world_settlement: WorldSettlementStatus::Settled,
        };
        let response = DurableResponse::RunIndexPage {
            page: DurableRunIndexPage {
                observed_revision: content_id("revision"),
                source_root: root_digest("root"),
                items: vec![item],
                next_cursor: None,
            },
        };
        response
            .verify_query_for(&run_index_query(
                Some(content_id("revision")),
                None,
                1,
                MAX_DURABLE_QUERY_PAGE_BYTES,
            ))
            .expect("exact request binding verifies");
        assert!(matches!(
            response.verify_query_for(&run_index_query(
                Some(content_id("other revision")),
                None,
                1,
                MAX_DURABLE_QUERY_PAGE_BYTES,
            )),
            Err(DurableError::Conflict { .. })
        ));

        let mut exceeds_limit = response.clone();
        let DurableResponse::RunIndexPage { page } = &mut exceeds_limit else {
            unreachable!("constructed Run index page")
        };
        page.items.push(DurableRunIndexSummary {
            run_id: "other-run".to_owned(),
            continuation_status: ContinuationStatus::Ready,
            execution_status: RunExecutionStatus::Active,
            world_settlement: WorldSettlementStatus::Settled,
        });
        page.items.sort_by_key(|item| {
            let position = DurablePagePosition::for_key(&item.run_id).expect("position derives");
            (position.key_hash, position.canonical_key)
        });
        assert!(
            exceeds_limit
                .verify_query_for(&run_index_query(
                    Some(content_id("revision")),
                    None,
                    1,
                    MAX_DURABLE_QUERY_PAGE_BYTES,
                ))
                .is_err()
        );

        let response_size = u64::try_from(
            cymule_core::canonical_bytes(&response)
                .expect("response canonicalizes")
                .len(),
        )
        .expect("response size fits u64");
        assert!(response_size > 1);
        assert!(
            response
                .verify_query_for(&run_index_query(
                    Some(content_id("revision")),
                    None,
                    1,
                    response_size - 1,
                ))
                .is_err()
        );

        assert_response_cursor_binding(response);
    }

    fn assert_response_cursor_binding(response: DurableResponse) {
        let same_item_cursor = cursor(DurablePageQueryKind::RunIndex, None, "run");
        assert!(
            response
                .verify_query_for(&run_index_query(
                    Some(same_item_cursor.source_revision.clone()),
                    Some(same_item_cursor),
                    1,
                    MAX_DURABLE_QUERY_PAGE_BYTES,
                ))
                .is_err()
        );

        let cursor = cursor(DurablePageQueryKind::RunIndex, None, "prior-run");
        let mut changed_revision_response = response.clone();
        let DurableResponse::RunIndexPage { page } = &mut changed_revision_response else {
            unreachable!("constructed Run index page")
        };
        page.observed_revision = content_id("changed revision");
        assert!(matches!(
            changed_revision_response.verify_query_for(&run_index_query(
                Some(cursor.source_revision.clone()),
                Some(cursor.clone()),
                1,
                MAX_DURABLE_QUERY_PAGE_BYTES,
            )),
            Err(DurableError::HistoryConflict { .. })
        ));

        let mut changed_root_response = response;
        let DurableResponse::RunIndexPage { page } = &mut changed_root_response else {
            unreachable!("constructed Run index page")
        };
        page.source_root = root_digest("changed root");
        assert!(matches!(
            changed_root_response.verify_query_for(&run_index_query(
                Some(cursor.source_revision.clone()),
                Some(cursor),
                1,
                MAX_DURABLE_QUERY_PAGE_BYTES,
            )),
            Err(DurableError::HistoryConflict { .. })
        ));
    }

    fn run_index_query(
        expected_revision: Option<String>,
        cursor: Option<DurablePageCursor>,
        limit: u32,
        max_canonical_bytes: u64,
    ) -> DurableCommand {
        DurableCommand::RunIndexPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
        }
    }

    #[test]
    fn exact_item_is_separate_run_bound_and_large_leaf_reachable() {
        assert!(
            MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES
                >= crate::MAX_STATE_ROOT_LEAF_BYTES as u64 + MAX_DURABLE_QUERY_PAGE_BYTES
        );
        let wait = pending_wait("run");
        let response = DurableResponse::RunItem {
            run_id: "run".to_owned(),
            observed_revision: content_id("revision"),
            source_root: root_digest("waits"),
            item: Some(Box::new(DurableRunItem::Wait {
                wait: Box::new(wait),
            })),
        };
        response.verify_wire().expect("exact wait verifies");
        response
            .verify_query_for(&DurableCommand::RunItem {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run".to_owned(),
                expected_revision: Some(content_id("revision")),
                selector: DurableRunItemSelector::Wait {
                    wait_id: content_id("wait"),
                },
                max_canonical_bytes: MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
            })
            .expect("exact selector binding verifies");
        assert!(
            response
                .verify_query_for(&DurableCommand::RunItem {
                    control_version: DURABLE_CONTROL_VERSION.to_owned(),
                    run_id: "run".to_owned(),
                    expected_revision: Some(content_id("revision")),
                    selector: DurableRunItemSelector::Wait {
                        wait_id: content_id("different wait"),
                    },
                    max_canonical_bytes: MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
                })
                .is_err()
        );

        let DurableResponse::RunItem { item, .. } = &response else {
            unreachable!("constructed exact response")
        };
        let mut foreign = item.as_ref().expect("item exists").as_ref().clone();
        let DurableRunItem::Wait { wait } = &mut foreign else {
            unreachable!("constructed wait item")
        };
        wait.run_id = "foreign-run".to_owned();
        let foreign_response = DurableResponse::RunItem {
            run_id: "run".to_owned(),
            observed_revision: content_id("revision"),
            source_root: root_digest("waits"),
            item: Some(Box::new(foreign)),
        };
        assert!(foreign_response.verify_wire().is_err());
    }

    #[test]
    fn applied_effect_summary_requires_a_result_and_accepts_canonical_null() -> DurableResult<()> {
        let mut summary = DurableEffectSummary {
            intent_id: content_id("effect:applied-null"),
            run_id: "run:applied-null".to_owned(),
            state: OutboxState::Applied,
            execution_availability: cymule_core::EffectExecutionAvailability::Available,
            reconciliation: cymule_core::ReconciliationState::Resolved,
            result: None,
        };
        assert!(summary.verify().is_err());
        summary.result = Some(cymule_core::artifact_ref(
            crate::model::EFFECT_RESULT_ARTIFACT_KIND,
            b"null",
        )?);
        summary.verify()?;
        let mut wrong_kind = summary.clone();
        wrong_kind.result = Some(cymule_core::artifact_ref(
            "cymule.test.wrong-effect-result/1",
            b"null",
        )?);
        assert!(wrong_kind.verify().is_err());
        let encoded = cymule_core::canonical_bytes(&summary)?;
        let reopened: DurableEffectSummary = cymule_core::decode_json(&encoded)?;
        reopened.verify()?;
        assert_eq!(reopened, summary);
        let fixture: DurableEffectSummary = cymule_core::decode_json(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/applied-effect-summary.json",
        )))?;
        fixture.verify()?;
        assert_eq!(fixture, summary);
        Ok(())
    }

    #[test]
    fn history_compaction_request_is_revision_pinned_and_not_a_wire_command() -> DurableResult<()> {
        let mut request = HistoryCompactionRequest {
            compaction_id: "compaction:request-shape".to_owned(),
            expected_revision: content_id("compaction-source"),
            kind: crate::HistoryCompactionKind::EventFreeAdmissions,
            requested_suffix: 0,
        };
        request.verify()?;
        request.requested_suffix = 1;
        assert!(request.verify().is_err());
        request.kind = crate::HistoryCompactionKind::EventPrefix;
        request.requested_suffix = crate::MAX_EXACT_INTEGER;
        request.verify()?;
        request.requested_suffix += 1;
        assert!(request.verify().is_err());
        request.requested_suffix = 0;
        request.expected_revision = "not-a-revision".to_owned();
        assert!(request.verify().is_err());
        request.expected_revision = content_id("compaction-source");
        request.compaction_id.clear();
        assert!(request.verify().is_err());
        assert!(
            cymule_core::decode_json::<DurableCommand>(
                br#"{
                "type":"compact_machine_history",
                "control_version":"cymule.durable-control/4"
            }"#
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn summaries_are_closed_small_and_lifecycle_consistent() {
        let mut run = DurableRunIndexSummary {
            run_id: "run".to_owned(),
            continuation_status: ContinuationStatus::Ready,
            execution_status: RunExecutionStatus::Active,
            world_settlement: WorldSettlementStatus::Settled,
        };
        run.verify().expect("active Run summary verifies");
        run.continuation_status = ContinuationStatus::Completed;
        assert!(run.verify().is_err());

        let wait = DurableWaitSummary {
            wait_id: content_id("wait"),
            run_id: "run".to_owned(),
            state: WaitState::Pending,
            result: None,
        };
        wait.verify().expect("pending wait summary verifies");
        assert!(
            cymule_core::canonical_bytes(&wait)
                .expect("summary canonicalizes")
                .len()
                <= MAX_DURABLE_QUERY_SUMMARY_BYTES
        );

        let mut invalid = wait;
        invalid.state = WaitState::Completed;
        assert!(invalid.verify().is_err());

        let mut value = serde_json::to_value(DurableRunItemSelector::Wait {
            wait_id: content_id("wait"),
        })
        .expect("selector serializes");
        value
            .as_object_mut()
            .expect("selector is object")
            .insert("unknown".to_owned(), Value::Null);
        assert!(serde_json::from_value::<DurableRunItemSelector>(value).is_err());
    }
}
