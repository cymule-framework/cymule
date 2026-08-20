use std::collections::BTreeSet;

use cymule_core::{
    ArtifactRef, COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus, DispatchPolicy,
    EffectPhase, EffectTransition, Machine, MutationKind, ReconciliationMode,
    ReconciliationResolution, ScopeStatus, WorldOutcome, effect_intent_id, effect_obligation_id,
};
use cymule_durable::{Continuation, DurableCoordinator, DurableStore, EffectDispatch, OutboxState};

use crate::{
    AgentError, AgentHost, AgentHostOccurrence, AgentHostOccurrenceState, AgentHostRequest,
    AgentHostResponse, AgentOccurrenceResolution, AgentOccurrenceStore, AgentResult,
    WorkspaceChange, WorkspaceReceipt,
    journal::{agent_occurrence_record, occurrence_journal_id},
};

/// Caller-owned identity and abstract effect site for one workspace overlay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceScopeRequest {
    /// Agent Session that owns the interaction occurrence.
    pub session_id: String,
    /// Semantic Run whose scope owns the overlay.
    pub run_id: String,
    /// Open scope to commit or abort.
    pub scope_id: String,
    /// Stable host occurrence identity supplied by the caller.
    pub occurrence_id: String,
    /// Provider-facing logical change identity.
    pub change_id: String,
    /// Immutable prepared overlay artifact.
    pub overlay: ArtifactRef,
    /// Abstract mutating effect contract declared by the Plan.
    pub operation: String,
    /// Stable invocation identity used by structural effect identity.
    pub invocation_id: String,
    /// Stable Plan site used by structural effect identity.
    pub site_id: String,
    /// Stable occurrence key within the site.
    pub occurrence_key: String,
}

impl WorkspaceScopeRequest {
    fn change(&self, commit: bool) -> WorkspaceChange {
        WorkspaceChange {
            change_id: self.change_id.clone(),
            overlay: self.overlay.clone(),
            commit,
        }
    }

    fn validate(&self) -> AgentResult<()> {
        for (kind, value) in [
            ("Session", self.session_id.as_str()),
            ("Run", self.run_id.as_str()),
            ("scope", self.scope_id.as_str()),
            ("occurrence", self.occurrence_id.as_str()),
            ("change", self.change_id.as_str()),
            ("operation", self.operation.as_str()),
            ("invocation", self.invocation_id.as_str()),
            ("site", self.site_id.as_str()),
            ("occurrence key", self.occurrence_key.as_str()),
        ] {
            if value.is_empty() {
                return Err(AgentError::Validation(format!(
                    "workspace {kind} identity must not be empty"
                )));
            }
        }
        self.overlay
            .validate()
            .map_err(|error| AgentError::Validation(error.to_string()))?;
        Ok(())
    }
}

/// Durable result of a workspace scope decision or reconciliation.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceScopeCheckpoint {
    /// Latest durable host occurrence snapshot.
    pub occurrence: AgentHostOccurrence,
    /// Retained provider receipt, when the provider produced one.
    pub receipt: Option<WorkspaceReceipt>,
    /// Structural mutating effect intent for a commit decision.
    pub effect_intent_id: Option<String>,
    /// Scope-transferred obligation for a commit decision.
    pub obligation_id: Option<String>,
    /// Whole-state M1 CAS revision containing this result.
    pub revision: String,
}

/// Durable, provider-neutral coupling between a workspace overlay and one scope.
///
/// The caller still owns the Agent or script loop. This controller only couples
/// a single workspace decision to core scope/effect semantics and the M1 CAS
/// boundary. Concrete filesystem, VCS, sandbox, and object-store behavior stays
/// behind `AgentHost`.
pub struct WorkspaceScopeController;

impl WorkspaceScopeController {
    /// Commit an overlay as a Plan-declared mutating Effect.
    ///
    /// Scope closure transfers an unresolved obligation before provider
    /// dispatch. The typed occurrence, outbox claim, Machine safe point, and
    /// Continuation projection are committed atomically around every external
    /// call boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when identities or the Plan contract are invalid, the
    /// scope/Continuation is stale, persistence conflicts, the provider fails,
    /// or an earlier dispatch requires reconciliation.
    pub fn commit<H: AgentHost, S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        host: &mut H,
        request: &WorkspaceScopeRequest,
    ) -> AgentResult<WorkspaceScopeCheckpoint> {
        request.validate()?;
        let change = request.change(true);
        let existing = load_occurrence(coordinator, request)?;
        if let Some(existing) = existing {
            ensure_request(&existing, &change)?;
            match existing.state {
                AgentHostOccurrenceState::Completed => {
                    return completed_commit_checkpoint(coordinator, request, existing);
                }
                AgentHostOccurrenceState::Prepared => {}
                AgentHostOccurrenceState::Started | AgentHostOccurrenceState::Unknown => {
                    return Err(AgentError::RecoveryRequired(format!(
                        "workspace commit {} may have reached its provider; reconcile the original occurrence",
                        request.occurrence_id
                    )));
                }
                AgentHostOccurrenceState::NotApplied => {
                    return Err(AgentError::RecoveryRequired(format!(
                        "workspace commit {} was not applied; use a new scope and occurrence for replacement work",
                        request.occurrence_id
                    )));
                }
            }
        } else {
            stage_commit(coordinator, host, request, change.clone())?;
        }

        let prepared = load_required_occurrence(coordinator, request)?;
        let mut machine = restore_machine(coordinator)?;
        let intent_id = workspace_intent_id(&machine, coordinator, request)?;
        let started = prepared.start()?;
        submit(
            &mut machine,
            &request.run_id,
            format!("{}:workspace-authorize", request.occurrence_id),
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        )?;
        submit(
            &mut machine,
            &request.run_id,
            format!("{}:workspace-dispatch-start", request.occurrence_id),
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::StartDispatch,
            },
        )?;
        let continuation = synced_continuation(coordinator, &machine, request, false)?;
        let claim_owner = claim_owner(request);
        coordinator
            .checkpoint_journal_effect_claim(
                &machine,
                continuation,
                &occurrence_journal_id(&request.session_id),
                &[agent_occurrence_record(&started)?],
                &intent_id,
                &claim_owner,
                claim_epoch(coordinator, request)?,
            )
            .map_err(persistence)?;

        match host.apply_workspace(change) {
            Ok(receipt) => settle_commit_receipt(coordinator, request, &started, receipt),
            Err(error) => {
                settle_commit_unknown(
                    coordinator,
                    request,
                    &started,
                    format!("workspace provider failed after dispatch: {error}"),
                )?;
                Err(error)
            }
        }
    }

    /// Abort an open scope only after the provider confirms the overlay was not
    /// committed.
    ///
    /// # Errors
    ///
    /// Returns an error when the scope is not current and open, persistence or
    /// the provider fails, the receipt is inconsistent, or recovery is required.
    pub fn abort<H: AgentHost, S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        host: &mut H,
        request: &WorkspaceScopeRequest,
    ) -> AgentResult<WorkspaceScopeCheckpoint> {
        request.validate()?;
        let change = request.change(false);
        let current = match load_occurrence(coordinator, request)? {
            Some(existing) => {
                ensure_request(&existing, &change)?;
                match existing.state {
                    AgentHostOccurrenceState::Completed => {
                        return finalize_abort(coordinator, request, existing);
                    }
                    AgentHostOccurrenceState::Prepared => existing,
                    AgentHostOccurrenceState::Started | AgentHostOccurrenceState::Unknown => {
                        return Err(AgentError::RecoveryRequired(format!(
                            "workspace abort {} may have reached its provider; reconcile the original occurrence",
                            request.occurrence_id
                        )));
                    }
                    AgentHostOccurrenceState::NotApplied => {
                        return Err(AgentError::RecoveryRequired(format!(
                            "workspace abort {} was not applied; the scope remains open",
                            request.occurrence_id
                        )));
                    }
                }
            }
            None => stage_abort(coordinator, host, request, change.clone())?,
        };
        let started = if current.state == AgentHostOccurrenceState::Prepared {
            let started = current.start()?;
            let machine = restore_machine(coordinator)?;
            let continuation = synced_continuation(coordinator, &machine, request, false)?;
            coordinator
                .checkpoint_machine_journal(
                    &machine,
                    continuation,
                    &occurrence_journal_id(&request.session_id),
                    &[agent_occurrence_record(&started)?],
                )
                .map_err(persistence)?;
            started
        } else {
            current
        };

        match host.apply_workspace(change) {
            Ok(receipt) => settle_abort_receipt(coordinator, request, &started, receipt),
            Err(error) => {
                settle_abort_unknown(
                    coordinator,
                    request,
                    &started,
                    format!("workspace provider failed after abort dispatch: {error}"),
                )?;
                Err(error)
            }
        }
    }

    /// Reconcile an ambiguous commit or abort against its original binding.
    ///
    /// # Errors
    ///
    /// Returns an error when the occurrence is missing or conflicting, the
    /// original provider cannot settle it, or the atomic settlement fails.
    pub fn reconcile<H: AgentHost, S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        host: &mut H,
        request: &WorkspaceScopeRequest,
    ) -> AgentResult<WorkspaceScopeCheckpoint> {
        request.validate()?;
        let current = load_required_occurrence(coordinator, request)?;
        let AgentHostRequest::Workspace(change) = &current.request else {
            return Err(AgentError::Validation(format!(
                "occurrence {} is not a workspace interaction",
                current.occurrence_id
            )));
        };
        ensure_request(&current, &request.change(change.commit))?;
        match current.state {
            AgentHostOccurrenceState::Completed if change.commit => {
                return completed_commit_checkpoint(coordinator, request, current);
            }
            AgentHostOccurrenceState::Completed => {
                return finalize_abort(coordinator, request, current);
            }
            AgentHostOccurrenceState::Prepared => {
                return Err(AgentError::IllegalTransition(format!(
                    "prepared workspace occurrence {} was not dispatched; resume its original decision",
                    current.occurrence_id
                )));
            }
            AgentHostOccurrenceState::NotApplied if !change.commit => {
                return Err(AgentError::RecoveryRequired(format!(
                    "workspace abort {} was not applied; the scope remains open",
                    current.occurrence_id
                )));
            }
            AgentHostOccurrenceState::NotApplied => {
                return completed_not_applied_commit(coordinator, request, current);
            }
            AgentHostOccurrenceState::Started | AgentHostOccurrenceState::Unknown => {}
        }

        match host.reconcile_occurrence(&current)? {
            AgentOccurrenceResolution::Completed { response } => {
                let AgentHostResponse::Workspace(receipt) = response else {
                    return Err(AgentError::Validation(
                        "workspace reconciliation returned a different response kind".to_owned(),
                    ));
                };
                if change.commit {
                    settle_commit_receipt(coordinator, request, &current, receipt)
                } else {
                    settle_abort_receipt(coordinator, request, &current, receipt)
                }
            }
            AgentOccurrenceResolution::NotApplied { evidence } if change.commit => {
                let not_applied = current.mark_not_applied(evidence)?;
                settle_commit_not_applied(coordinator, request, not_applied)
            }
            AgentOccurrenceResolution::NotApplied { evidence } => {
                let not_applied = current.mark_not_applied(evidence)?;
                let machine = restore_machine(coordinator)?;
                let continuation = synced_continuation(coordinator, &machine, request, false)?;
                coordinator
                    .checkpoint_machine_journal(
                        &machine,
                        continuation,
                        &occurrence_journal_id(&request.session_id),
                        &[agent_occurrence_record(&not_applied)?],
                    )
                    .map_err(persistence)?;
                Err(AgentError::RecoveryRequired(format!(
                    "workspace abort {} was not applied; the scope remains open",
                    request.occurrence_id
                )))
            }
            AgentOccurrenceResolution::Unknown { evidence } if change.commit => {
                if current.state == AgentHostOccurrenceState::Started {
                    let unknown = current.mark_unknown_with_evidence(
                        "workspace reconciliation could not determine the original commit",
                        evidence,
                    )?;
                    settle_commit_unknown_occurrence(coordinator, request, &unknown)?;
                }
                Err(AgentError::RecoveryRequired(format!(
                    "workspace commit {} remains unknown",
                    request.occurrence_id
                )))
            }
            AgentOccurrenceResolution::Unknown { evidence } => {
                if current.state == AgentHostOccurrenceState::Started {
                    let unknown = current.mark_unknown_with_evidence(
                        "workspace reconciliation could not determine the original abort",
                        evidence,
                    )?;
                    checkpoint_abort_occurrence(coordinator, request, &unknown)?;
                }
                Err(AgentError::RecoveryRequired(format!(
                    "workspace abort {} remains unknown",
                    request.occurrence_id
                )))
            }
        }
    }
}

fn stage_commit<H: AgentHost, S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    host: &mut H,
    request: &WorkspaceScopeRequest,
    change: WorkspaceChange,
) -> AgentResult<()> {
    let mut machine = restore_machine(coordinator)?;
    validate_open_scope(coordinator, &machine, request)?;
    validate_commit_contract(&machine, request)?;
    let host_request = AgentHostRequest::Workspace(change);
    let binding = host.bind_occurrence(&host_request)?;
    let prepared = AgentHostOccurrence::prepare(
        &request.occurrence_id,
        &request.session_id,
        host_request,
        binding.clone(),
    )?;
    let intent_id = workspace_intent_id(&machine, coordinator, request)?;
    let continuation = continuation(coordinator, request)?;
    let frame = continuation
        .frames
        .iter()
        .rev()
        .find(|frame| frame.invocation_id == request.invocation_id)
        .ok_or_else(|| {
            AgentError::IllegalTransition(format!(
                "workspace invocation {} is not active",
                request.invocation_id
            ))
        })?;
    submit(
        &mut machine,
        &request.run_id,
        format!("{}:workspace-effect-propose", request.occurrence_id),
        Command::ProposeEffect {
            scope_id: request.scope_id.clone(),
            invocation_id: request.invocation_id.clone(),
            invocation_path: frame.invocation_path.clone(),
            definition_id: frame.definition_id.clone(),
            region_path: frame.region_path.clone(),
            site_id: request.site_id.clone(),
            occurrence: request.occurrence_key.clone(),
            operation: request.operation.clone(),
            args: request.overlay.clone(),
            occurrence_binding: binding.clone(),
        },
    )?;
    submit(
        &mut machine,
        &request.run_id,
        format!("{}:workspace-effect-prepare", request.occurrence_id),
        Command::TransitionEffect {
            intent_id: intent_id.clone(),
            transition: EffectTransition::Prepare,
        },
    )?;
    submit(
        &mut machine,
        &request.run_id,
        format!("{}:workspace-scope-commit", request.occurrence_id),
        Command::CommitScope {
            scope_id: request.scope_id.clone(),
        },
    )?;
    let continuation = synced_continuation(coordinator, &machine, request, true)?;
    coordinator
        .checkpoint_journal_effect_enqueue(
            &machine,
            continuation,
            &occurrence_journal_id(&request.session_id),
            &[agent_occurrence_record(&prepared)?],
            EffectDispatch {
                intent_id,
                run_id: request.run_id.clone(),
                operation: request.operation.clone(),
                input: request.overlay.clone(),
                occurrence_binding: binding,
                state: OutboxState::Pending,
                claim_epoch: 0,
                claim_owner: None,
                result: None,
            },
        )
        .map_err(persistence)?;
    Ok(())
}

fn stage_abort<H: AgentHost, S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    host: &mut H,
    request: &WorkspaceScopeRequest,
    change: WorkspaceChange,
) -> AgentResult<AgentHostOccurrence> {
    let machine = restore_machine(coordinator)?;
    validate_open_scope(coordinator, &machine, request)?;
    let host_request = AgentHostRequest::Workspace(change);
    let binding = host.bind_occurrence(&host_request)?;
    let prepared = AgentHostOccurrence::prepare(
        &request.occurrence_id,
        &request.session_id,
        host_request,
        binding,
    )?;
    let continuation = synced_continuation(coordinator, &machine, request, false)?;
    coordinator
        .checkpoint_machine_journal(
            &machine,
            continuation,
            &occurrence_journal_id(&request.session_id),
            &[agent_occurrence_record(&prepared)?],
        )
        .map_err(persistence)?;
    Ok(prepared)
}

fn settle_commit_receipt<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    current: &AgentHostOccurrence,
    receipt: WorkspaceReceipt,
) -> AgentResult<WorkspaceScopeCheckpoint> {
    if coordinator
        .restore_machine()
        .map_err(persistence)?
        .artifact(&receipt.evidence)
        .is_none()
    {
        settle_commit_unknown(
            coordinator,
            request,
            current,
            "workspace receipt references evidence absent from the canonical artifact store"
                .to_owned(),
        )?;
        return Err(AgentError::Validation(
            "workspace receipt evidence is not a canonical artifact".to_owned(),
        ));
    }
    let completed = match current.complete(AgentHostResponse::Workspace(receipt.clone())) {
        Ok(completed) => completed,
        Err(error) => {
            settle_commit_unknown(
                coordinator,
                request,
                current,
                format!("workspace provider returned an invalid commit receipt: {error}"),
            )?;
            return Err(error);
        }
    };
    let outcome = if receipt.committed {
        WorldOutcome::Applied
    } else {
        WorldOutcome::NotApplied
    };
    let outbox = if receipt.committed {
        OutboxState::Applied
    } else {
        OutboxState::NotApplied
    };
    settle_commit_terminal(
        coordinator,
        request,
        completed,
        outcome,
        outbox,
        Some(receipt.evidence.clone()),
        Some(receipt),
    )
}

fn settle_commit_not_applied<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    occurrence: AgentHostOccurrence,
) -> AgentResult<WorkspaceScopeCheckpoint> {
    settle_commit_terminal(
        coordinator,
        request,
        occurrence,
        WorldOutcome::NotApplied,
        OutboxState::NotApplied,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn settle_commit_terminal<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    occurrence: AgentHostOccurrence,
    outcome: WorldOutcome,
    outbox: OutboxState,
    result: Option<ArtifactRef>,
    receipt: Option<WorkspaceReceipt>,
) -> AgentResult<WorkspaceScopeCheckpoint> {
    let mut machine = restore_machine(coordinator)?;
    let intent_id = workspace_intent_id(&machine, coordinator, request)?;
    observe_or_reconcile(&mut machine, request, &intent_id, outcome)?;
    let continuation = synced_continuation(coordinator, &machine, request, false)?;
    let (owner, epoch) = outbox_claim(coordinator, &intent_id)?;
    let revision = coordinator
        .checkpoint_journal_effect_settlement(
            &machine,
            continuation,
            &occurrence_journal_id(&request.session_id),
            &[agent_occurrence_record(&occurrence)?],
            &intent_id,
            &owner,
            epoch,
            outbox,
            result,
        )
        .map_err(persistence)?;
    Ok(WorkspaceScopeCheckpoint {
        occurrence,
        receipt,
        effect_intent_id: Some(intent_id.clone()),
        obligation_id: Some(effect_obligation_id(&intent_id).map_err(core_validation)?),
        revision,
    })
}

fn settle_commit_unknown<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    current: &AgentHostOccurrence,
    failure: String,
) -> AgentResult<String> {
    let unknown = current.mark_unknown(failure)?;
    settle_commit_unknown_occurrence(coordinator, request, &unknown)
}

fn settle_commit_unknown_occurrence<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    unknown: &AgentHostOccurrence,
) -> AgentResult<String> {
    let mut machine = restore_machine(coordinator)?;
    let intent_id = workspace_intent_id(&machine, coordinator, request)?;
    observe_or_reconcile(&mut machine, request, &intent_id, WorldOutcome::Unknown)?;
    let continuation = synced_continuation(coordinator, &machine, request, false)?;
    let (owner, epoch) = outbox_claim(coordinator, &intent_id)?;
    coordinator
        .checkpoint_journal_effect_settlement(
            &machine,
            continuation,
            &occurrence_journal_id(&request.session_id),
            &[agent_occurrence_record(unknown)?],
            &intent_id,
            &owner,
            epoch,
            OutboxState::Unknown,
            None,
        )
        .map_err(persistence)
}

fn settle_abort_receipt<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    current: &AgentHostOccurrence,
    receipt: WorkspaceReceipt,
) -> AgentResult<WorkspaceScopeCheckpoint> {
    if receipt.committed {
        settle_abort_unknown(
            coordinator,
            request,
            current,
            "workspace abort returned a receipt claiming the overlay was committed".to_owned(),
        )?;
        return Err(AgentError::Validation(
            "workspace abort cannot accept a committed receipt".to_owned(),
        ));
    }
    let machine = restore_machine(coordinator)?;
    if machine.artifact(&receipt.evidence).is_none() {
        settle_abort_unknown(
            coordinator,
            request,
            current,
            "workspace abort receipt references non-canonical evidence".to_owned(),
        )?;
        return Err(AgentError::Validation(
            "workspace abort receipt evidence is not a canonical artifact".to_owned(),
        ));
    }
    let completed = match current.complete(AgentHostResponse::Workspace(receipt)) {
        Ok(completed) => completed,
        Err(error) => {
            settle_abort_unknown(
                coordinator,
                request,
                current,
                format!("workspace provider returned an invalid abort receipt: {error}"),
            )?;
            return Err(error);
        }
    };
    finalize_abort(coordinator, request, completed)
}

fn finalize_abort<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    occurrence: AgentHostOccurrence,
) -> AgentResult<WorkspaceScopeCheckpoint> {
    let receipt = workspace_receipt(&occurrence)?.clone();
    if receipt.committed {
        return Err(AgentError::IllegalTransition(
            "a committed workspace receipt cannot finalize scope abort".to_owned(),
        ));
    }
    let mut machine = restore_machine(coordinator)?;
    let scope = machine
        .projection()
        .runs
        .get(&request.run_id)
        .and_then(|run| run.scopes.get(&request.scope_id))
        .ok_or_else(|| {
            AgentError::NotFound(format!("scope {} does not exist", request.scope_id))
        })?;
    if scope.status == ScopeStatus::ClosedAborted {
        return Ok(WorkspaceScopeCheckpoint {
            occurrence,
            receipt: Some(receipt),
            effect_intent_id: None,
            obligation_id: None,
            revision: current_revision(coordinator)?,
        });
    }
    if scope.status == ScopeStatus::Open {
        submit(
            &mut machine,
            &request.run_id,
            format!("{}:workspace-scope-abort", request.occurrence_id),
            Command::AbortScope {
                scope_id: request.scope_id.clone(),
            },
        )?;
    } else {
        return Err(AgentError::IllegalTransition(format!(
            "scope {} was committed and cannot accept an abort receipt",
            request.scope_id
        )));
    }
    let continuation = synced_continuation(coordinator, &machine, request, true)?;
    let revision = coordinator
        .checkpoint_machine_journal(
            &machine,
            continuation,
            &occurrence_journal_id(&request.session_id),
            &[agent_occurrence_record(&occurrence)?],
        )
        .map_err(persistence)?;
    Ok(WorkspaceScopeCheckpoint {
        occurrence,
        receipt: Some(receipt),
        effect_intent_id: None,
        obligation_id: None,
        revision,
    })
}

fn settle_abort_unknown<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    current: &AgentHostOccurrence,
    failure: String,
) -> AgentResult<String> {
    let unknown = current.mark_unknown(failure)?;
    checkpoint_abort_occurrence(coordinator, request, &unknown)
}

fn checkpoint_abort_occurrence<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    occurrence: &AgentHostOccurrence,
) -> AgentResult<String> {
    let machine = restore_machine(coordinator)?;
    let continuation = synced_continuation(coordinator, &machine, request, false)?;
    coordinator
        .checkpoint_machine_journal(
            &machine,
            continuation,
            &occurrence_journal_id(&request.session_id),
            &[agent_occurrence_record(occurrence)?],
        )
        .map_err(persistence)
}

fn completed_commit_checkpoint<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    occurrence: AgentHostOccurrence,
) -> AgentResult<WorkspaceScopeCheckpoint> {
    let receipt = workspace_receipt(&occurrence)?.clone();
    let machine = restore_machine(coordinator)?;
    let intent_id = workspace_intent_id(&machine, coordinator, request)?;
    let effect = &machine.projection().runs[&request.run_id].effects[&intent_id];
    let expected = if receipt.committed {
        WorldOutcome::Applied
    } else {
        WorldOutcome::NotApplied
    };
    if effect.outcome != expected {
        return Err(AgentError::Persistence(format!(
            "workspace occurrence {} and effect {intent_id} disagree",
            request.occurrence_id
        )));
    }
    let obligation_id = effect_obligation_id(&intent_id).map_err(core_validation)?;
    if !machine.projection().runs[&request.run_id]
        .obligations
        .get(&obligation_id)
        .is_some_and(|obligation| obligation.resolved)
    {
        return Err(AgentError::Persistence(format!(
            "workspace effect {intent_id} has no resolved scope obligation"
        )));
    }
    Ok(WorkspaceScopeCheckpoint {
        occurrence,
        receipt: Some(receipt),
        effect_intent_id: Some(intent_id),
        obligation_id: Some(obligation_id),
        revision: current_revision(coordinator)?,
    })
}

fn completed_not_applied_commit<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
    occurrence: AgentHostOccurrence,
) -> AgentResult<WorkspaceScopeCheckpoint> {
    let machine = restore_machine(coordinator)?;
    let intent_id = workspace_intent_id(&machine, coordinator, request)?;
    if machine.projection().runs[&request.run_id].effects[&intent_id].outcome
        != WorldOutcome::NotApplied
    {
        return Err(AgentError::Persistence(format!(
            "workspace occurrence {} is not_applied but effect {intent_id} is not",
            request.occurrence_id
        )));
    }
    let obligation_id = effect_obligation_id(&intent_id).map_err(core_validation)?;
    if !machine.projection().runs[&request.run_id]
        .obligations
        .get(&obligation_id)
        .is_some_and(|obligation| obligation.resolved)
        || coordinator
            .state()
            .map_err(persistence)?
            .outbox
            .get(&intent_id)
            .is_none_or(|dispatch| dispatch.state != OutboxState::NotApplied)
    {
        return Err(AgentError::Persistence(format!(
            "workspace effect {intent_id} has inconsistent not-applied settlement"
        )));
    }
    Ok(WorkspaceScopeCheckpoint {
        occurrence,
        receipt: None,
        effect_intent_id: Some(intent_id.clone()),
        obligation_id: Some(obligation_id),
        revision: current_revision(coordinator)?,
    })
}

fn validate_open_scope<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    machine: &Machine,
    request: &WorkspaceScopeRequest,
) -> AgentResult<()> {
    if machine.artifact(&request.overlay).is_none() {
        return Err(AgentError::NotFound(format!(
            "workspace overlay artifact {} does not exist",
            request.overlay.artifact_id
        )));
    }
    let run = machine
        .projection()
        .runs
        .get(&request.run_id)
        .ok_or_else(|| AgentError::NotFound(format!("Run {} does not exist", request.run_id)))?;
    let scope = run.scopes.get(&request.scope_id).ok_or_else(|| {
        AgentError::NotFound(format!("scope {} does not exist", request.scope_id))
    })?;
    if scope.status != ScopeStatus::Open {
        return Err(AgentError::IllegalTransition(format!(
            "workspace scope {} is not open",
            request.scope_id
        )));
    }
    let continuation = continuation(coordinator, request)?;
    if continuation.epoch != run.epoch || continuation.scope_stack.last() != Some(&request.scope_id)
    {
        return Err(AgentError::IllegalTransition(
            "workspace scope is not the current fenced Continuation scope".to_owned(),
        ));
    }
    Ok(())
}

fn validate_commit_contract(machine: &Machine, request: &WorkspaceScopeRequest) -> AgentResult<()> {
    let run = &machine.projection().runs[&request.run_id];
    let contract = machine
        .plan(&run.current_plan)
        .and_then(|plan| {
            plan.candidate
                .effects
                .iter()
                .find(|contract| contract.id == request.operation)
        })
        .ok_or_else(|| {
            AgentError::NotFound(format!(
                "workspace effect contract {} does not exist",
                request.operation
            ))
        })?;
    if contract.profile.mutation != MutationKind::Mutating
        || contract.profile.dispatch != DispatchPolicy::OnScopeCommit
        || contract.profile.reconciliation != ReconciliationMode::Queryable
        || !contract.profile.keyed_idempotency
    {
        return Err(AgentError::Validation(
            "workspace commit requires a mutating on_scope_commit, queryable, keyed-idempotent effect contract"
                .to_owned(),
        ));
    }
    Ok(())
}

fn workspace_intent_id<S: DurableStore>(
    machine: &Machine,
    coordinator: &DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
) -> AgentResult<String> {
    let run = machine
        .projection()
        .runs
        .get(&request.run_id)
        .ok_or_else(|| AgentError::NotFound(format!("Run {} does not exist", request.run_id)))?;
    let continuation = continuation(coordinator, request)?;
    if continuation.epoch != run.epoch {
        return Err(AgentError::IllegalTransition(
            "workspace Continuation epoch is stale".to_owned(),
        ));
    }
    effect_intent_id(
        &request.run_id,
        &request.invocation_id,
        &request.site_id,
        &request.scope_id,
        run.epoch,
        &request.occurrence_key,
        &request.overlay,
        "cymule.effect-schema/1",
    )
    .map_err(core_validation)
}

fn synced_continuation<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    machine: &Machine,
    request: &WorkspaceScopeRequest,
    close_scope: bool,
) -> AgentResult<Continuation> {
    let mut continuation = continuation(coordinator, request)?.clone();
    let run = machine
        .projection()
        .runs
        .get(&request.run_id)
        .ok_or_else(|| AgentError::NotFound(format!("Run {} does not exist", request.run_id)))?;
    if close_scope {
        match continuation.scope_stack.last() {
            Some(scope) if scope == &request.scope_id => {
                continuation.scope_stack.pop();
            }
            None => {}
            Some(_) if !continuation.scope_stack.contains(&request.scope_id) => {}
            Some(_) => {
                return Err(AgentError::IllegalTransition(
                    "workspace scope is not the current Continuation scope".to_owned(),
                ));
            }
        }
    }
    continuation.plan_id.clone_from(&run.current_plan);
    continuation
        .binding_context
        .clone_from(&run.current_binding_context);
    continuation.epoch = run.epoch;
    continuation.effect_obligations = run
        .obligations
        .values()
        .filter(|obligation| obligation.blocking && !obligation.resolved)
        .map(|obligation| obligation.obligation_id.clone())
        .collect();
    continuation.causal_frontier = BTreeSet::from([run.last_event.clone()]);
    Ok(continuation)
}

fn observe_or_reconcile(
    machine: &mut Machine,
    request: &WorkspaceScopeRequest,
    intent_id: &str,
    outcome: WorldOutcome,
) -> AgentResult<()> {
    let effect = machine.projection().runs[&request.run_id].effects[intent_id].clone();
    let transition = match effect.outcome {
        WorldOutcome::Unobserved if effect.phase == EffectPhase::DispatchStarted => {
            EffectTransition::Observe(outcome)
        }
        WorldOutcome::Unknown => EffectTransition::Reconcile(match outcome {
            WorldOutcome::Applied => ReconciliationResolution::ResolvedApplied,
            WorldOutcome::NotApplied => ReconciliationResolution::ResolvedNotApplied,
            WorldOutcome::Unknown => ReconciliationResolution::StillUnknown,
            WorldOutcome::Unobserved => {
                return Err(AgentError::Validation(
                    "workspace settlement cannot return to unobserved".to_owned(),
                ));
            }
        }),
        existing if existing == outcome => return Ok(()),
        existing => {
            return Err(AgentError::IllegalTransition(format!(
                "workspace effect {intent_id} is already {existing:?}, not {outcome:?}"
            )));
        }
    };
    submit(
        machine,
        &request.run_id,
        format!(
            "{}:workspace-effect-{}",
            request.occurrence_id,
            outcome_suffix(outcome)
        ),
        Command::TransitionEffect {
            intent_id: intent_id.to_owned(),
            transition,
        },
    )
}

fn outcome_suffix(outcome: WorldOutcome) -> &'static str {
    match outcome {
        WorldOutcome::Applied => "applied",
        WorldOutcome::NotApplied => "not-applied",
        WorldOutcome::Unknown => "unknown",
        WorldOutcome::Unobserved => "unobserved",
    }
}

fn submit(
    machine: &mut Machine,
    run_id: &str,
    command_id: String,
    command: Command,
) -> AgentResult<()> {
    let expected_precondition = machine
        .projection()
        .runs
        .get(run_id)
        .map(cymule_core::RunProjection::precondition_token);
    let receipt = machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id,
            actor: "actor:agent-workspace".to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition,
            command,
        })
        .map_err(core_validation)?;
    if receipt.status != CommandReceiptStatus::Applied {
        return Err(AgentError::IllegalTransition(format!(
            "workspace command conflicted at {:?}",
            receipt.current_precondition
        )));
    }
    Ok(())
}

fn ensure_request(occurrence: &AgentHostOccurrence, expected: &WorkspaceChange) -> AgentResult<()> {
    if occurrence.request != AgentHostRequest::Workspace(expected.clone()) {
        return Err(AgentError::IllegalTransition(format!(
            "workspace occurrence {} was reused with a different decision",
            occurrence.occurrence_id
        )));
    }
    Ok(())
}

fn workspace_receipt(occurrence: &AgentHostOccurrence) -> AgentResult<&WorkspaceReceipt> {
    match occurrence.response.as_ref() {
        Some(AgentHostResponse::Workspace(receipt)) => Ok(receipt),
        _ => Err(AgentError::Persistence(format!(
            "workspace occurrence {} has no retained receipt",
            occurrence.occurrence_id
        ))),
    }
}

fn continuation<'a, S: DurableStore>(
    coordinator: &'a DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
) -> AgentResult<&'a Continuation> {
    coordinator
        .state()
        .map_err(persistence)?
        .continuations
        .get(&request.run_id)
        .ok_or_else(|| {
            AgentError::NotFound(format!("Continuation {} does not exist", request.run_id))
        })
}

fn load_occurrence<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
) -> AgentResult<Option<AgentHostOccurrence>> {
    Ok(
        AgentOccurrenceStore::load_occurrences(coordinator, &request.session_id)?
            .into_iter()
            .find(|occurrence| occurrence.occurrence_id == request.occurrence_id),
    )
}

fn load_required_occurrence<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
) -> AgentResult<AgentHostOccurrence> {
    load_occurrence(coordinator, request)?.ok_or_else(|| {
        AgentError::NotFound(format!(
            "workspace occurrence {} does not exist",
            request.occurrence_id
        ))
    })
}

fn restore_machine<S: DurableStore>(coordinator: &DurableCoordinator<S>) -> AgentResult<Machine> {
    coordinator.restore_machine().map_err(persistence)
}

fn outbox_claim<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    intent_id: &str,
) -> AgentResult<(String, u64)> {
    let dispatch = coordinator
        .state()
        .map_err(persistence)?
        .outbox
        .get(intent_id)
        .ok_or_else(|| AgentError::NotFound(format!("effect {intent_id} is not in the outbox")))?;
    let owner = dispatch.claim_owner.clone().ok_or_else(|| {
        AgentError::Persistence(format!("workspace effect {intent_id} has no claim owner"))
    })?;
    Ok((owner, dispatch.claim_epoch))
}

fn claim_owner(request: &WorkspaceScopeRequest) -> String {
    format!("dispatcher:agent-workspace:{}", request.session_id)
}

fn claim_epoch<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    request: &WorkspaceScopeRequest,
) -> AgentResult<u64> {
    Ok(continuation(coordinator, request)?.epoch)
}

fn current_revision<S: DurableStore>(coordinator: &DurableCoordinator<S>) -> AgentResult<String> {
    coordinator
        .revision()
        .map(str::to_owned)
        .ok_or_else(|| AgentError::Persistence("durable state is not initialized".to_owned()))
}

fn persistence(error: impl std::fmt::Display) -> AgentError {
    AgentError::Persistence(error.to_string())
}

fn core_validation(error: impl std::fmt::Display) -> AgentError {
    AgentError::Validation(error.to_string())
}
