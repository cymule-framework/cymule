use super::{
    Command, CommandEnvelope, CoreError, Event, MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
    MachineAuthorityFrontier, MachineLogRoot, MachineMapRoot, MachinePagedReadInputs,
    MachinePagedTransitionCurrent, MachinePagedTransitionPhase, MachineRunCurrent,
    MachineRunIndexSelector, MachineRunLogSelector, MachineRunReadInputs, MachineRunReadSet,
    MachineScopeCurrent, PinnedRunReduction, Result, account_paged_read_budget, canonical_digest,
    expected_paged_read_keys, finalize_paged_scope_action, prepare_paged_transition_context,
    reduce_paged_effect_page, require_childless_open_scope, validate_paged_read_leaves,
    verify_effect_read,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(super) struct BatchReadContext {
    pub batch_id: String,
    pub position: u32,
    pub length: u32,
}

/// Exact bounded Scope sources required by one command inside an atomic batch.
///
/// This is a local resolver request, not a serializable execution authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineInlineScopeReadRequirement {
    /// Exact Scope being closed.
    pub scope_id: String,
    /// Complete membership index selected by the closure action.
    pub index_selector: MachineRunIndexSelector,
    /// Pinned source membership root.
    pub index_root: MachineMapRoot,
    /// Complete proposal-order log selected by the closure action.
    pub log_selector: MachineRunLogSelector,
    /// Pinned source proposal-order root.
    pub log_root: MachineLogRoot,
}

impl MachineInlineScopeReadRequirement {
    fn from_scope(envelope: &CommandEnvelope, scope: &MachineScopeCurrent) -> Result<Self> {
        let (scope_id, commit) = match &envelope.command {
            Command::CommitScope { scope_id } => (scope_id, true),
            Command::AbortScope { scope_id } => (scope_id, false),
            _ => {
                return Err(CoreError::Validation(
                    "inline Scope closure requires a Scope command".to_owned(),
                ));
            }
        };
        scope.verify()?;
        if &scope.scope_id != scope_id {
            return Err(CoreError::IdentityMismatch(
                "inline Scope closure changed its target Scope".to_owned(),
            ));
        }
        require_childless_open_scope(scope)?;
        if commit {
            Ok(Self {
                scope_id: scope_id.clone(),
                index_selector: MachineRunIndexSelector::ScopeMutatingEffects {
                    scope_id: scope_id.clone(),
                },
                index_root: scope.mutating_effects.clone(),
                log_selector: MachineRunLogSelector::ScopeMutatingEffects {
                    scope_id: scope_id.clone(),
                },
                log_root: scope.mutating_effect_order.clone(),
            })
        } else {
            if scope.abort_blockers.entries != 0 {
                return Err(CoreError::IllegalTransition(format!(
                    "scope {scope_id} cannot abort after effect release"
                )));
            }
            Ok(Self {
                scope_id: scope_id.clone(),
                index_selector: MachineRunIndexSelector::ScopeEffects {
                    scope_id: scope_id.clone(),
                },
                index_root: scope.effects.clone(),
                log_selector: MachineRunLogSelector::ScopeEffects {
                    scope_id: scope_id.clone(),
                },
                log_root: scope.effect_order.clone(),
            })
        }
    }

    fn fits_inline_bound(&self) -> bool {
        usize::try_from(self.index_root.entries)
            .is_ok_and(|count| count <= MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES)
    }

    fn paged_required(&self, run_id: &str) -> CoreError {
        CoreError::PagedScopeRequired {
            run_id: run_id.to_owned(),
            scope_id: self.scope_id.clone(),
            entries: self.index_root.entries,
        }
    }
}

pub(super) fn scope_read_requirement(
    context: Option<&BatchReadContext>,
    envelope: &CommandEnvelope,
    scope: &MachineScopeCurrent,
) -> Result<Option<MachineInlineScopeReadRequirement>> {
    let Some(context) = context else {
        return Ok(None);
    };
    if !matches!(
        envelope.command,
        Command::CommitScope { .. } | Command::AbortScope { .. }
    ) {
        return Ok(None);
    }
    let requirement = MachineInlineScopeReadRequirement::from_scope(envelope, scope)?;
    if !requirement.fits_inline_bound() {
        if context.length > 1 {
            return Err(requirement.paged_required(&envelope.run_id));
        }
        return Ok(None);
    }
    Ok(Some(requirement))
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct InlineScopeClosure {
    pub envelope: CommandEnvelope,
    pub requirement: MachineInlineScopeReadRequirement,
    pub effect_ids: Vec<String>,
    pub obligation_ids: BTreeSet<String>,
}

impl InlineScopeClosure {
    pub(super) fn verify(
        envelope: &CommandEnvelope,
        requirement: MachineInlineScopeReadRequirement,
        inputs: &MachineRunReadInputs,
    ) -> Result<Self> {
        if inputs.run_id != envelope.run_id {
            return Err(CoreError::IdentityMismatch(
                "inline Scope closure changed its target Run".to_owned(),
            ));
        }
        let scope = inputs
            .scopes
            .get(&requirement.scope_id)
            .and_then(Option::as_ref)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine inline Scope current",
                key: requirement.scope_id.clone(),
            })?;
        if requirement != MachineInlineScopeReadRequirement::from_scope(envelope, scope)? {
            return Err(CoreError::IdentityMismatch(
                "inline Scope closure does not select its exact current sources".to_owned(),
            ));
        }
        if !requirement.fits_inline_bound() {
            return Err(requirement.paged_required(&envelope.run_id));
        }
        let effect_ids = verify_complete_scope_pages(envelope, &requirement, inputs)?;
        let obligation_ids =
            verify_inline_scope_effects(envelope, &requirement, &effect_ids, inputs)?;
        Ok(Self {
            envelope: envelope.clone(),
            requirement,
            effect_ids,
            obligation_ids,
        })
    }
}

fn verify_complete_scope_pages(
    envelope: &CommandEnvelope,
    requirement: &MachineInlineScopeReadRequirement,
    inputs: &MachineRunReadInputs,
) -> Result<Vec<String>> {
    let [index] = inputs.index_pages.as_slice() else {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine inline Scope complete index page",
            key: requirement.scope_id.clone(),
        });
    };
    let [log] = inputs.log_pages.as_slice() else {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine inline Scope complete order page",
            key: requirement.scope_id.clone(),
        });
    };
    index.verify_local()?;
    log.verify_local()?;
    if index.run_id != envelope.run_id
        || index.selector() != &requirement.index_selector
        || index.source() != &requirement.index_root
        || index.cursor().is_some()
        || index.next_cursor().is_some()
        || u64::try_from(index.entries().len()).ok() != Some(requirement.index_root.entries)
        || log.run_id != envelope.run_id
        || log.selector() != &requirement.log_selector
        || log.source() != &requirement.log_root
        || log.start() != 0
        || log.end()? != requirement.log_root.len
        || !log.is_terminal()?
        || requirement.index_root.entries != requirement.log_root.len
        || index.entries().iter().collect::<BTreeSet<_>>()
            != log.entries().iter().collect::<BTreeSet<_>>()
    {
        return Err(CoreError::IdentityMismatch(
            "inline Scope pages do not prove the same complete pinned membership and order"
                .to_owned(),
        ));
    }
    Ok(log.entries().to_vec())
}

fn verify_inline_scope_effects(
    envelope: &CommandEnvelope,
    requirement: &MachineInlineScopeReadRequirement,
    effect_ids: &[String],
    inputs: &MachineRunReadInputs,
) -> Result<BTreeSet<String>> {
    let expected_effects = effect_ids.iter().cloned().collect::<BTreeSet<_>>();
    if inputs.effects.keys().ne(expected_effects.iter()) {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine inline Scope Effect leaves",
            key: requirement.scope_id.clone(),
        });
    }
    let mut obligations = BTreeSet::new();
    for (id, effect) in &inputs.effects {
        let effect = effect
            .as_ref()
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine inline Scope Effect leaf",
                key: id.clone(),
            })?;
        verify_effect_read(effect)?;
        if effect.intent_id != *id || effect.scope_id != requirement.scope_id {
            return Err(CoreError::IdentityMismatch(
                "inline Scope Effect leaf changed its identity or owning Scope".to_owned(),
            ));
        }
        if matches!(envelope.command, Command::CommitScope { .. }) {
            obligations.insert(crate::machine::obligation_for_effect(effect)?.obligation_id);
        }
    }
    if inputs.obligations.keys().ne(obligations.iter()) {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine inline Scope obligation leaves",
            key: requirement.scope_id.clone(),
        });
    }
    for (id, obligation) in &inputs.obligations {
        if obligation.is_some() {
            return Err(CoreError::IllegalTransition(format!(
                "obligation {id} already exists"
            )));
        }
    }
    Ok(obligations)
}

pub(super) fn reduce_inline_scope(
    reads: &MachineRunReadSet,
    event: &Event,
    frontier: &MachineAuthorityFrontier,
) -> Result<PinnedRunReduction> {
    let closure =
        reads
            .inline_scope
            .as_ref()
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine inline Scope closure",
                key: event.command_id.clone(),
            })?;
    if event.run_id != closure.envelope.run_id
        || event.command_id != closure.envelope.command_id
        || event.command_hash != canonical_digest(&closure.envelope)?
    {
        return Err(CoreError::IdentityMismatch(
            "inline Scope Event does not bind its admitted command".to_owned(),
        ));
    }
    let transition = prepare_paged_transition_context(frontier, reads, closure.envelope.clone())?;
    let mut next = transition.clone();
    let mut current = reads
        .inputs
        .run
        .clone()
        .ok_or_else(|| CoreError::NotFound(format!("Run {} does not exist", event.run_id)))?;
    let mut reduction = PinnedRunReduction::default();
    if !closure.effect_ids.is_empty() {
        reduce_inline_effect_page(
            reads,
            closure,
            &transition,
            &mut reduction,
            &mut current,
            &mut next,
        )?;
    }
    next.processed_count = transition.effect_source.len;
    next.phase = MachinePagedTransitionPhase::Finalize;
    next.next_index = 0;
    let scopes = reads
        .inputs
        .scopes
        .iter()
        .map(|(id, scope)| {
            scope
                .clone()
                .map(|scope| (id.clone(), scope))
                .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                    family: "Machine inline Scope final leaf",
                    key: id.clone(),
                })
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let payload = finalize_paged_scope_action(&next, &scopes, &mut reduction, &mut current)?;
    if payload != event.payload {
        return Err(CoreError::IdentityMismatch(
            "inline Scope closure did not reproduce its admitted Event payload".to_owned(),
        ));
    }
    current.world_settlement = current.indexes.settlement();
    current.last_event.clone_from(&event.event_id);
    reduction.result_current = Some(current);
    Ok(reduction)
}

fn reduce_inline_effect_page(
    reads: &MachineRunReadSet,
    closure: &InlineScopeClosure,
    transition: &MachinePagedTransitionCurrent,
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    next: &mut MachinePagedTransitionCurrent,
) -> Result<()> {
    let [page] = reads.inputs.log_pages.as_slice() else {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine inline Scope complete order page",
            key: closure.requirement.scope_id.clone(),
        });
    };
    let scope = reads.require_scope(&closure.requirement.scope_id)?.clone();
    let effects = closure
        .effect_ids
        .iter()
        .map(|id| {
            reads
                .inputs
                .effects
                .get(id)
                .and_then(Option::as_ref)
                .cloned()
                .map(|effect| (id.clone(), effect))
                .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                    family: "Machine inline Scope Effect leaf",
                    key: id.clone(),
                })
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let inputs = MachinePagedReadInputs::new(
        current.clone(),
        page.clone(),
        BTreeMap::from([(scope.scope_id.clone(), scope)]),
        effects,
        reads.inputs.obligations.clone(),
    );
    let (expected_scopes, expected_obligations) = expected_paged_read_keys(transition, &inputs)?;
    validate_paged_read_leaves(transition, &inputs, &expected_scopes, &expected_obligations)?;
    account_paged_read_budget(transition, &inputs)?;
    reduce_paged_effect_page(transition, &inputs, reduction, current, next)
}

#[cfg(test)]
mod tests {
    include!("inline_scope_tests.rs");
}
