//! Explicit offline maintenance over one resolver-authenticated Core source.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::super::{
    CoreError, Machine, MachineCompaction, MachineDelta, MachineRootDelta, MachineRootParts,
    Result, current_command_index_root,
};
use super::MachineAuthorityFrontier;

/// Exact hot-history cut requested by framework-owned offline maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MachineCompactionIntent {
    /// Archive the complete Event/admission prefix before this retained suffix.
    EventPrefix {
        /// Number of complete hot Events to retain after the cut.
        retain_suffix: usize,
    },
    /// Archive the complete nonempty, Event-free conflict/material admission tail.
    EventFreeAdmissions,
}

/// Core-derived offline compaction, ready for one exact-source Store CAS.
///
/// This value has no public constructor or decoder. It preserves the semantic
/// frontier while replacing only the authenticated physical base and archive
/// cut. The embedding must publish its archive, root delta, and frontier together.
#[derive(Debug)]
pub struct PreparedPinnedMachineCompaction {
    frontier: MachineAuthorityFrontier,
    root_delta: MachineRootDelta,
    compaction: MachineCompaction,
}

impl PreparedPinnedMachineCompaction {
    /// Result frontier with unchanged semantic authority and cumulative counts.
    pub fn frontier(&self) -> &MachineAuthorityFrontier {
        &self.frontier
    }

    /// Exact Core-derived physical removals, replacement base, and retained proofs.
    pub fn root_delta(&self) -> &MachineRootDelta {
        &self.root_delta
    }

    /// Independent archive object and causal-cut evidence for the same CAS.
    pub fn compaction(&self) -> &MachineCompaction {
        &self.compaction
    }
}

/// Prepare an explicit offline compaction from one pinned, authenticated source.
///
/// The embedding must assemble `source` directly from the roots authenticated by
/// `frontier` for this maintenance operation; neither value is a public caller
/// input. This path deliberately replays the complete hot Core source once and
/// may process the complete new base. It is not an ordinary bounded transition
/// or an implicit archive-hydration path. A compacted source uses only its exact
/// pinned base anchor, never a fallback traversal of cold archive objects.
///
/// # Errors
///
/// Returns an error when source authority disagrees, any paged command remains
/// pending, the requested cut is empty or not causally closed, or compaction
/// would change semantic authority. No external mutation is performed.
pub fn prepare_pinned_compaction(
    frontier: &MachineAuthorityFrontier,
    source: MachineRootParts,
    intent: MachineCompactionIntent,
) -> Result<PreparedPinnedMachineCompaction> {
    let mut machine = restore_compaction_source(frontier, source)?;
    let compaction = match intent {
        MachineCompactionIntent::EventPrefix { retain_suffix } => {
            machine.compact_event_history(retain_suffix)?
        }
        MachineCompactionIntent::EventFreeAdmissions => machine.compact_event_free_admissions()?,
    };
    if machine.authority_root()? != frontier.authority_root {
        return Err(CoreError::IdentityMismatch(
            "offline Machine compaction changed semantic authority".to_owned(),
        ));
    }
    let anchor = machine.base_anchor.as_ref().ok_or_else(|| {
        CoreError::IdentityMismatch("offline Machine compaction has no result anchor".to_owned())
    })?;
    let mut result = frontier.clone();
    result.base_anchor_id = Some(anchor.anchor_id.clone());
    result
        .command_index_root
        .clone_from(&anchor.command_index_root);
    result.verify()?;
    let root_delta = compaction_root_delta(frontier, &result, machine, &compaction)?;
    Ok(PreparedPinnedMachineCompaction {
        frontier: result,
        root_delta,
        compaction,
    })
}

fn restore_compaction_source(
    frontier: &MachineAuthorityFrontier,
    source: MachineRootParts,
) -> Result<Machine> {
    frontier.verify()?;
    if frontier.pending_commands.entries != 0 || frontier.paged_transitions.entries != 0 {
        return Err(CoreError::Causal(
            "offline Machine compaction requires no pending paged commands".to_owned(),
        ));
    }
    source.verify_keys()?;
    let anchor = source.base_anchor.clone();
    if anchor.as_ref().map(|anchor| anchor.anchor_id.as_str()) != frontier.base_anchor_id.as_deref()
        || current_command_index_root(source.base.as_ref())? != frontier.command_index_root
    {
        return Err(CoreError::IdentityMismatch(
            "offline Machine source does not match the pinned base anchor or command index"
                .to_owned(),
        ));
    }
    let snapshot = source.into_snapshot_unchecked();
    let machine = match anchor {
        Some(anchor) => Machine::restore_anchored(snapshot, &anchor)?,
        None => Machine::restore(snapshot)?,
    };
    // Both roots use the shared semantic preimage, including every cumulative
    // material/batch/Event count and the exact command-admission chain head.
    if machine.authority_root()? != frontier.authority_root {
        return Err(CoreError::IdentityMismatch(
            "offline Machine source does not match the pinned semantic frontier".to_owned(),
        ));
    }
    // Physical roots are deliberately outside the semantic digest. Check their
    // cardinalities against the Projection already restored from this source.
    for (kind, entries, projected) in [
        ("Run", frontier.runs.entries, machine.projection.runs.len()),
        (
            "Fact",
            frontier.facts.entries,
            machine.projection.facts.len(),
        ),
    ] {
        let projected =
            u64::try_from(projected).map_err(|error| CoreError::Validation(error.to_string()))?;
        if entries != projected {
            return Err(CoreError::IdentityMismatch(format!(
                "offline Machine {kind} count {entries} does not match restored Projection count {projected}"
            )));
        }
    }
    Ok(machine)
}

fn compaction_root_delta(
    source: &MachineAuthorityFrontier,
    result: &MachineAuthorityFrontier,
    mut machine: Machine,
    compaction: &MachineCompaction,
) -> Result<MachineRootDelta> {
    let segment = &compaction.archive_segment;
    let command_ids: BTreeSet<_> = segment
        .entries
        .iter()
        .map(|entry| entry.command.envelope.command_id.clone())
        .collect();
    let base = machine.base.take().ok_or_else(|| {
        CoreError::IdentityMismatch("offline Machine compaction has no result base".to_owned())
    })?;
    Ok(MachineRootDelta {
        root_delta_version: MachineRootDelta::VERSION.to_owned(),
        delta_version: MachineDelta::VERSION.to_owned(),
        parent_authority_root: source.authority_root.clone(),
        result_authority_root: result.authority_root.clone(),
        parent_anchor_id: source.base_anchor_id.clone(),
        result_anchor_id: result.base_anchor_id.clone(),
        plans: BTreeMap::new(),
        plan_admission_order: Vec::new(),
        artifacts: BTreeMap::new(),
        artifact_admission_order: Vec::new(),
        batches: BTreeMap::new(),
        batch_admission_order: Vec::new(),
        removed_event_ids: segment
            .entries
            .iter()
            .flat_map(|entry| entry.events.iter().map(|event| event.event_id.clone()))
            .collect(),
        removed_admission_ids: segment
            .entries
            .iter()
            .map(|entry| entry.admission.admission_id.clone())
            .collect(),
        removed_command_ids: command_ids.clone(),
        removed_batch_ids: segment
            .batches
            .iter()
            .map(|batch| batch.batch_id.clone())
            .collect(),
        removed_command_index_proof_ids: command_ids,
        base: Some(Arc::unwrap_or_clone(base)),
        base_anchor: machine.base_anchor,
        archive_segment: Some(segment.header.clone()),
        events: Vec::new(),
        admissions: Vec::new(),
        commands: BTreeMap::new(),
        command_index_proofs: machine.command_index_proofs,
    })
}
