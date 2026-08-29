use super::{
    AdmissionCommitment, ArchivedCommandRecord, ArtifactRecord, ArtifactRef,
    COMMAND_ADMISSION_VERSION, CommandAdmission, CommandAdmissionParent, CommandEnvelope,
    CommandReceipt, CommandReceiptStatus, CommandRecord, CoreError, EffectIntentIdentityInput,
    Event, EventContent, EventPayload, MACHINE_ARTIFACT_ADMISSION_COMMITMENT_DOMAIN,
    MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN, MACHINE_COMMAND_BATCH_VERSION,
    MACHINE_PLAN_ADMISSION_COMMITMENT_DOMAIN, Machine, MachineAuthorityRootInput,
    MachineCommandArchiveEntry, MachineCommandBatchMaterialSource, MachineCommandBatchMember,
    MachineCommandBatchRecord, MachineCommandIndexProof, MachineDelta, MachineRootDelta,
    ObligationProjection, OpenScopeEffectIndex, PROJECTION_ROOT_EVENT_DOMAIN,
    PROJECTION_ROOT_GENESIS_DOMAIN, ROOT_SCOPE_ID, Result, RunDerivedIndex, SealedPlan,
    canonical_digest, command_intent_hash, command_material_membership, content_id,
    deserialize_required_nullable, effect_intent_id, effect_obligation_id, footprints,
    is_canonical_digest, machine_authority_root, machine_command_batch_id, plan_invocation_id,
    single_command_batch_metadata, validate_envelope, validate_identity, verify_admission_record,
    verify_event_footprint,
};
use crate::Command;
use cymule_authenticated_collections::{
    LogRangeProof, MapPosition, MapRangeProof, VerifiedLogRange, VerifiedMapPage, verify_log_range,
    verify_map_range,
};
pub use cymule_authenticated_collections::{LogRoot as MachineLogRoot, MapRoot as MachineMapRoot};
use std::collections::{BTreeMap, BTreeSet};

#[path = "inline_scope.rs"]
mod inline_scope;
pub use inline_scope::MachineInlineScopeReadRequirement;
use inline_scope::{BatchReadContext, InlineScopeClosure};

#[path = "pinned_compaction.rs"]
mod compaction;
pub use compaction::{
    MachineCompactionIntent, PreparedPinnedMachineCompaction, prepare_pinned_compaction,
};

/// Maximum identities returned by one exact persistent-index page.
pub const MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES: usize = 256;

/// Maximum index pages admitted by one ordinary command-shaped read set.
///
/// An ordinary reduction may consume at most one complete page for each exact
/// selector. Work which cannot fit in that page enters the explicit persisted
/// page-transition protocol instead of extending this bound.
pub const MAX_PINNED_MACHINE_INDEX_PAGES: usize = 8;

/// Maximum canonical bytes admitted for one exact keyed read.
pub const MAX_PINNED_MACHINE_READ_LEAF_BYTES: usize = 12 * 1024 * 1024;

/// Maximum aggregate canonical bytes admitted by one ordinary read set.
pub const MAX_PINNED_MACHINE_READ_SET_BYTES: usize = 64 * 1024 * 1024;

/// Maximum typed leaves admitted by one ordinary command-shaped read set.
///
/// This is a per-reduction I/O bound, not a Run-size bound. Commands which
/// affect more identities use an explicit revision-pinned page state machine.
pub const MAX_PINNED_MACHINE_READ_SET_ENTRIES: usize = 1_024;
/// Dynamic values in one complete inline Scope map/log/effect/obligation witness.
pub const MAX_INLINE_SCOPE_DYNAMIC_ENTRIES: usize = 4 * MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES;
/// Exact target Scope and its optional direct parent, counted separately.
pub const MAX_INLINE_SCOPE_STRUCTURAL_ENTRIES: usize = 2;

const MACHINE_AUTHORITY_FRONTIER_VERSION: &str = "cymule.machine-authority-frontier/3";
const MACHINE_RUN_CURRENT_VERSION: &str = "cymule.machine-run-current/2";
const MACHINE_SCOPE_CURRENT_VERSION: &str = "cymule.machine-scope-current/1";
const MACHINE_INDEX_MEMBERSHIP_VALUE_VERSION: &str = "cymule.machine-index-membership-value/1";
const MACHINE_ORDER_ENTRY_VALUE_VERSION: &str = "cymule.machine-order-entry-value/1";
const MACHINE_PAGED_TRANSITION_VERSION: &str = "cymule.machine-paged-transition/1";
const MACHINE_RUN_PLAN_LINEAGE_DOMAIN: &str = "cymule.machine-run-plan-lineage/1";
const MACHINE_RUN_BINDING_LINEAGE_DOMAIN: &str = "cymule.machine-run-binding-lineage/1";
const MACHINE_SCOPE_EFFECT_LINEAGE_DOMAIN: &str = "cymule.machine-scope-effect-lineage/1";
const MACHINE_SCOPE_MUTATING_EFFECT_LINEAGE_DOMAIN: &str =
    "cymule.machine-scope-mutating-effect-lineage/1";
const MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN: &str = "cymule.machine-paged-processed-lineage/1";
const MACHINE_PAGED_ACTION_ID_DOMAIN: &str = "cymule.machine-paged-action/1";
const PINNED_ROOT_MUTATION_DIGEST_DOMAIN: &str = "cymule.pinned-root-mutation/1";
const PREPARED_PAGED_BEGIN_AUTHORITY_DOMAIN: &str = "cymule.prepared-paged-begin/1";
const PREPARED_PAGED_FINALIZE_AUTHORITY_DOMAIN: &str = "cymule.prepared-paged-finalize/1";
const PREPARED_PAGED_STEP_AUTHORITY_DOMAIN: &str = "cymule.prepared-paged-step/1";
const PREPARED_PINNED_READ_COMMAND_AUTHORITY_DOMAIN: &str = "cymule.prepared-pinned-read-command/1";
const PREPARED_PINNED_RUN_LOOKUP_AUTHORITY_DOMAIN: &str = "cymule.prepared-pinned-run-lookup/1";
const MACHINE_MATERIAL_ADMISSION_DOMAIN: &str = "cymule.machine-material-admission/1";

/// Maximum Plans in one framework-owned immutable-material admission.
pub const MAX_MACHINE_MATERIAL_PLANS: usize = 64;
/// Maximum Artifacts in one framework-owned immutable-material admission.
pub const MAX_MACHINE_MATERIAL_ARTIFACTS: usize = 256;
/// Maximum commands in one atomic pinned batch.
pub const MAX_PINNED_COMMAND_BATCH_COMMANDS: usize = super::MAX_MACHINE_COMMAND_BATCH_MEMBERS;

fn lineage_genesis(domain: &str) -> Result<String> {
    content_id(domain, &("genesis", ()))
}

fn lineage_append(domain: &str, parent: &str, identity: &str) -> Result<String> {
    crate::validate_content_id("Machine lineage parent", parent)?;
    crate::validate_content_id("Machine lineage identity", identity)?;
    content_id(domain, &("append", parent, identity))
}

/// Fixed global reducer frontier pinned by the durable Store head.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineAuthorityFrontier {
    /// Frontier schema generation.
    pub frontier_version: String,
    /// Core-owned semantic authority root recomputed from the fixed fields.
    pub authority_root: String,
    /// Core-owned rolling commitment over unique Plan admission order.
    pub plan_admission_commitment: String,
    /// Number of retained Plans.
    pub plan_count: u64,
    /// Core-owned rolling commitment over unique Artifact admission order.
    pub artifact_admission_commitment: String,
    /// Number of retained Artifacts.
    pub artifact_count: u64,
    /// Core-owned rolling commitment over atomic command-batch admission order.
    pub batch_admission_commitment: String,
    /// Number of retained command batches.
    pub batch_count: u64,
    /// Event-chain projection root.
    pub projection_root: String,
    /// Number of admitted Events.
    pub event_count: u64,
    /// Last command-admission sequence, or zero at genesis.
    pub admission_sequence: u64,
    /// Last command-admission identity, null at genesis.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub admission_head: Option<String>,
    /// Current compacted-base anchor identity, null before compaction.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub base_anchor_id: Option<String>,
    /// Current cumulative archived-command index root.
    pub command_index_root: String,
    /// Store-owned keyed Run-current map.
    pub runs: MachineMapRoot,
    /// Store-owned append-only fact map.
    pub facts: MachineMapRoot,
    /// Store-owned global command reservation map for in-progress paged work.
    pub pending_commands: MachineMapRoot,
    /// Store-owned persisted paged-transition map.
    pub paged_transitions: MachineMapRoot,
}

impl MachineAuthorityFrontier {
    /// Construct the unique empty semantic frontier over caller-supplied empty
    /// physical Run and fact roots.
    ///
    /// # Errors
    ///
    /// Returns an error unless every supplied root is a valid empty physical
    /// authority or Core cannot derive the canonical semantic genesis.
    pub fn genesis(
        runs: MachineMapRoot,
        facts: MachineMapRoot,
        pending_commands: MachineMapRoot,
        paged_transitions: MachineMapRoot,
    ) -> Result<Self> {
        runs.verify()?;
        facts.verify()?;
        pending_commands.verify()?;
        paged_transitions.verify()?;
        if runs.entries != 0
            || facts.entries != 0
            || pending_commands.entries != 0
            || paged_transitions.entries != 0
        {
            return Err(CoreError::Validation(
                "Machine genesis requires empty physical Run and fact roots".to_owned(),
            ));
        }
        let plans = AdmissionCommitment::new(MACHINE_PLAN_ADMISSION_COMMITMENT_DOMAIN);
        let artifacts = AdmissionCommitment::new(MACHINE_ARTIFACT_ADMISSION_COMMITMENT_DOMAIN);
        let batches = AdmissionCommitment::new(MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN);
        let projection_root = canonical_digest(&(PROJECTION_ROOT_GENESIS_DOMAIN, ()))?;
        let command_index_root = MachineCommandIndexProof::empty_root()?;
        let mut frontier = Self {
            frontier_version: MACHINE_AUTHORITY_FRONTIER_VERSION.to_owned(),
            authority_root: String::new(),
            plan_admission_commitment: plans.root().to_owned(),
            plan_count: 0,
            artifact_admission_commitment: artifacts.root().to_owned(),
            artifact_count: 0,
            batch_admission_commitment: batches.root().to_owned(),
            batch_count: 0,
            projection_root,
            event_count: 0,
            admission_sequence: 0,
            admission_head: None,
            base_anchor_id: None,
            command_index_root,
            runs,
            facts,
            pending_commands,
            paged_transitions,
        };
        frontier.authority_root = frontier.expected_authority_root()?;
        frontier.verify()?;
        Ok(frontier)
    }

    /// Verify all fixed fields and recompute the Core-owned authority root.
    ///
    /// # Errors
    ///
    /// Returns an error when any count, root, lineage field, or the declared
    /// semantic authority root is invalid.
    pub fn verify(&self) -> Result<()> {
        if self.frontier_version != MACHINE_AUTHORITY_FRONTIER_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported Machine authority frontier version {:?}",
                self.frontier_version
            )));
        }
        for (kind, value) in [
            ("Machine authority root", self.authority_root.as_str()),
            ("Machine projection root", self.projection_root.as_str()),
        ] {
            if !is_canonical_digest(value) {
                return Err(CoreError::Validation(format!(
                    "{kind} must be a lowercase SHA-256 digest"
                )));
            }
        }
        for (kind, value) in [
            (
                "Machine Plan admission commitment",
                self.plan_admission_commitment.as_str(),
            ),
            (
                "Machine Artifact admission commitment",
                self.artifact_admission_commitment.as_str(),
            ),
            (
                "Machine command-batch admission commitment",
                self.batch_admission_commitment.as_str(),
            ),
            (
                "Machine command-index root",
                self.command_index_root.as_str(),
            ),
        ] {
            crate::validate_content_id(kind, value)?;
        }
        for (kind, count) in [
            ("Plan", self.plan_count),
            ("Artifact", self.artifact_count),
            ("command batch", self.batch_count),
            ("Event", self.event_count),
            ("admission", self.admission_sequence),
        ] {
            if count > crate::MAX_EXACT_INTEGER {
                return Err(CoreError::Validation(format!(
                    "Machine {kind} count exceeds the exact integer range"
                )));
            }
        }
        match (&self.admission_head, self.admission_sequence) {
            (None, 0) => {}
            (Some(head), sequence) if sequence > 0 => {
                crate::validate_content_id("Machine admission head", head)?;
            }
            _ => {
                return Err(CoreError::Validation(
                    "Machine admission head and sequence disagree".to_owned(),
                ));
            }
        }
        if let Some(anchor) = &self.base_anchor_id {
            crate::validate_content_id("Machine base anchor", anchor)?;
        } else if self.command_index_root != MachineCommandIndexProof::empty_root()? {
            return Err(CoreError::Validation(
                "uncompacted Machine has a non-empty archived-command root".to_owned(),
            ));
        }
        self.runs.verify()?;
        self.facts.verify()?;
        self.pending_commands.verify()?;
        self.paged_transitions.verify()?;
        if self.pending_commands.entries != self.paged_transitions.entries {
            return Err(CoreError::Validation(
                "Machine pending-command and paged-transition counts disagree".to_owned(),
            ));
        }
        let expected = self.expected_authority_root()?;
        if self.authority_root != expected {
            return Err(CoreError::IdentityMismatch(format!(
                "Machine authority frontier root {} does not match {expected}",
                self.authority_root
            )));
        }
        Ok(())
    }

    fn expected_authority_root(&self) -> Result<String> {
        machine_authority_root(&MachineAuthorityRootInput {
            plan_commitment: &self.plan_admission_commitment,
            plan_count: self.plan_count,
            artifact_commitment: &self.artifact_admission_commitment,
            artifact_count: self.artifact_count,
            batch_commitment: &self.batch_admission_commitment,
            batch_count: self.batch_count,
            projection_root: &self.projection_root,
            event_count: self.event_count,
            admission_sequence: (self.admission_sequence != 0).then_some(self.admission_sequence),
            admission_head: self.admission_head.as_deref(),
        })
    }
}

/// Physical roots of the four unbounded keyed Run child collections.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineRunChildRoots {
    /// Scope-current leaves.
    pub scopes: MachineMapRoot,
    /// Effect-current leaves.
    pub effects: MachineMapRoot,
    /// Obligation-current leaves.
    pub obligations: MachineMapRoot,
    /// Attempt-current leaves.
    pub attempts: MachineMapRoot,
}

impl MachineRunChildRoots {
    fn verify(&self) -> Result<()> {
        self.scopes.verify()?;
        self.effects.verify()?;
        self.obligations.verify()?;
        self.attempts.verify()?;
        Ok(())
    }
}

/// Physical proposal-order logs required for bounded query and transition
/// paging. These logs never replace keyed membership maps.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineRunOrderRoots {
    /// Scope creation order.
    pub scopes: MachineLogRoot,
    /// Effect proposal order.
    pub effects: MachineLogRoot,
    /// Obligation creation order.
    pub obligations: MachineLogRoot,
    /// Attempt creation order.
    pub attempts: MachineLogRoot,
    /// Plan migration lineage.
    pub plans: MachineLogRoot,
    /// Binding migration lineage.
    pub bindings: MachineLogRoot,
}

impl MachineRunOrderRoots {
    fn verify(&self) -> Result<()> {
        self.scopes.verify()?;
        self.effects.verify()?;
        self.obligations.verify()?;
        self.attempts.verify()?;
        self.plans.verify()?;
        self.bindings.verify()?;
        Ok(())
    }
}

/// One physical root for each unbounded reducer membership index.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineRunIndexRoots {
    /// Effects requiring governance.
    pub governance_effects: MachineMapRoot,
    /// Effects with unknown world outcomes.
    pub unknown_effects: MachineMapRoot,
    /// Effects not yet settled.
    pub pending_effects: MachineMapRoot,
    /// Effects changed by Run termination.
    pub terminal_transition_effects: MachineMapRoot,
    /// Currently open scopes.
    pub open_scopes: MachineMapRoot,
    /// Unresolved blocking obligations.
    pub unresolved_obligations: MachineMapRoot,
}

impl MachineRunIndexRoots {
    fn verify(&self) -> Result<()> {
        self.governance_effects.verify()?;
        self.unknown_effects.verify()?;
        self.pending_effects.verify()?;
        self.terminal_transition_effects.verify()?;
        self.open_scopes.verify()?;
        self.unresolved_obligations.verify()?;
        Ok(())
    }

    fn settlement(&self) -> crate::WorldSettlementStatus {
        if self.governance_effects.entries != 0 {
            crate::WorldSettlementStatus::GovernanceRequired
        } else if self.unknown_effects.entries != 0 {
            crate::WorldSettlementStatus::Unknown
        } else if self.pending_effects.entries != 0 {
            crate::WorldSettlementStatus::Pending
        } else {
            crate::WorldSettlementStatus::Settled
        }
    }
}

/// Closed reducer fence for one Run current.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum MachineRunReducerState {
    /// The Run may admit a normal command.
    Ready,
    /// One revision-pinned multi-page transition exclusively owns the Run.
    Transitioning {
        /// Exact persisted transition identity.
        transition_id: String,
    },
}

impl MachineRunReducerState {
    fn verify(&self) -> Result<()> {
        match self {
            Self::Ready => Ok(()),
            Self::Transitioning { transition_id } => {
                crate::validate_content_id("Machine paged transition", transition_id)
            }
        }
    }
}

/// Scalar current authority for one Run.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineRunCurrent {
    /// Current schema generation.
    pub run_current_version: String,
    /// Owning Run identity.
    pub run_id: String,
    /// Initial immutable Plan.
    pub initial_plan: String,
    /// Current immutable Plan.
    pub current_plan: String,
    /// Core-owned append commitment over the complete Plan lineage.
    pub plan_lineage_root: String,
    /// Complete Plan-lineage length.
    pub plan_lineage_count: u64,
    /// Physical proposal-order Plan lineage.
    pub plan_lineage: MachineLogRoot,
    /// Initial execution binding.
    pub initial_binding_context: String,
    /// Current execution binding.
    pub current_binding_context: String,
    /// Core-owned append commitment over the complete binding lineage.
    pub binding_lineage_root: String,
    /// Complete binding-lineage length.
    pub binding_lineage_count: u64,
    /// Physical proposal-order binding lineage.
    pub binding_lineage: MachineLogRoot,
    /// Current execution epoch.
    pub epoch: u64,
    /// Canonical execution status.
    pub execution_status: crate::RunExecutionStatus,
    /// Canonical world-settlement summary.
    pub world_settlement: crate::WorldSettlementStatus,
    /// Optional terminal result.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub result: Option<ArtifactRef>,
    /// Exact last Event on this Run's frontier.
    pub last_event: String,
    /// The sole currently active Attempt, if any.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub active_attempt_id: Option<String>,
    /// Total number of committed Effects.
    pub committed_effect_count: u64,
    /// Closed normal-command or paged-transition fence.
    pub reducer_state: MachineRunReducerState,
    /// Exact unbounded child-map roots.
    pub children: MachineRunChildRoots,
    /// Exact proposal-order child and lineage logs.
    pub order: MachineRunOrderRoots,
    /// Exact unbounded reducer-index roots.
    pub indexes: MachineRunIndexRoots,
}

impl MachineRunCurrent {
    /// Current schema selector.
    pub const VERSION: &'static str = MACHINE_RUN_CURRENT_VERSION;

    /// Return the stale-action token without loading any child leaf.
    pub fn precondition_token(&self) -> String {
        format!("pre:{}:{}", self.epoch, self.last_event)
    }

    /// Verify bounded scalar authority independently of child values.
    ///
    /// # Errors
    ///
    /// Returns an error when identity, count, physical-root, or execution-state
    /// authority is internally inconsistent.
    pub fn verify(&self) -> Result<()> {
        self.verify_identity_and_counts()?;
        self.verify_physical_authority()?;
        self.verify_execution_authority()
    }

    fn verify_identity_and_counts(&self) -> Result<()> {
        if self.run_current_version != MACHINE_RUN_CURRENT_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported Machine Run-current version {:?}",
                self.run_current_version
            )));
        }
        validate_identity("Machine Run", &self.run_id)?;
        for (kind, value) in [
            ("initial Plan", self.initial_plan.as_str()),
            ("current Plan", self.current_plan.as_str()),
            ("Plan-lineage root", self.plan_lineage_root.as_str()),
            (
                "initial execution binding",
                self.initial_binding_context.as_str(),
            ),
            (
                "current execution binding",
                self.current_binding_context.as_str(),
            ),
            ("binding-lineage root", self.binding_lineage_root.as_str()),
            ("last Event", self.last_event.as_str()),
        ] {
            crate::validate_content_id(kind, value)?;
        }
        if self.plan_lineage_count == 0
            || self.binding_lineage_count == 0
            || self.plan_lineage_count > crate::MAX_EXACT_INTEGER
            || self.binding_lineage_count > crate::MAX_EXACT_INTEGER
            || self.epoch > crate::MAX_EXACT_INTEGER
            || self.committed_effect_count > crate::MAX_EXACT_INTEGER
        {
            return Err(CoreError::Validation(
                "Machine Run-current scalar count exceeds its closed range".to_owned(),
            ));
        }
        if let Some(attempt) = &self.active_attempt_id {
            crate::validate_content_id("active Attempt", attempt)?;
        }
        if let Some(result) = &self.result {
            result.validate()?;
        }
        self.reducer_state.verify()?;
        Ok(())
    }

    fn verify_physical_authority(&self) -> Result<()> {
        self.children.verify()?;
        self.order.verify()?;
        self.indexes.verify()?;
        if self.plan_lineage_count != self.plan_lineage.len
            || self.binding_lineage_count != self.binding_lineage.len
            || self.plan_lineage != self.order.plans
            || self.binding_lineage != self.order.bindings
            || self.children.scopes.entries != self.order.scopes.len
            || self.children.effects.entries != self.order.effects.len
            || self.children.obligations.entries != self.order.obligations.len
            || self.children.attempts.entries != self.order.attempts.len
            || self.indexes.open_scopes.entries > self.children.scopes.entries
            || self.indexes.unresolved_obligations.entries > self.children.obligations.entries
            || self.committed_effect_count > self.children.effects.entries
        {
            return Err(CoreError::Validation(
                "Machine Run-current map, log, lineage, or index counts disagree".to_owned(),
            ));
        }
        if self.children.scopes.entries == 0 {
            return Err(CoreError::Validation(
                "Machine Run-current has no root Scope".to_owned(),
            ));
        }
        if self.active_attempt_id.is_some() && self.children.attempts.entries == 0 {
            return Err(CoreError::Validation(
                "Machine Run-current active Attempt is absent from its child map".to_owned(),
            ));
        }
        if self.world_settlement != self.indexes.settlement() {
            return Err(CoreError::Validation(
                "Machine Run settlement does not match its pinned index roots".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_execution_authority(&self) -> Result<()> {
        let has_open_scope = self.indexes.open_scopes.entries != 0;
        let has_active_attempt = self.active_attempt_id.is_some();
        match &self.execution_status {
            crate::RunExecutionStatus::Active => {
                if self.result.is_some() {
                    return Err(CoreError::Validation(
                        "active Machine Run-current carries a terminal Result".to_owned(),
                    ));
                }
            }
            crate::RunExecutionStatus::Completed => {
                if has_open_scope
                    || has_active_attempt
                    || self.indexes.unresolved_obligations.entries != 0
                    || self.indexes.terminal_transition_effects.entries != 0
                    || self.world_settlement != crate::WorldSettlementStatus::Settled
                    || !matches!(self.reducer_state, MachineRunReducerState::Ready)
                {
                    return Err(CoreError::Validation(
                        "completed Machine Run-current retains live reducer work".to_owned(),
                    ));
                }
            }
            crate::RunExecutionStatus::Failed { failure } => {
                failure.verify()?;
                verify_terminal_run_current(self, has_open_scope, has_active_attempt)?;
            }
            crate::RunExecutionStatus::Cancelled { reason } => {
                reason.validate()?;
                verify_terminal_run_current(self, has_open_scope, has_active_attempt)?;
            }
        }
        if matches!(
            self.reducer_state,
            MachineRunReducerState::Transitioning { .. }
        ) && !matches!(self.execution_status, crate::RunExecutionStatus::Active)
        {
            return Err(CoreError::Validation(
                "terminal Machine Run-current cannot retain a paged transition fence".to_owned(),
            ));
        }
        Ok(())
    }
}

fn verify_terminal_run_current(
    current: &MachineRunCurrent,
    has_open_scope: bool,
    has_active_attempt: bool,
) -> Result<()> {
    if current.epoch == 0
        || current.result.is_some()
        || has_open_scope
        || has_active_attempt
        || current.indexes.pending_effects.entries != 0
        || current.indexes.terminal_transition_effects.entries != 0
        || !matches!(current.reducer_state, MachineRunReducerState::Ready)
    {
        return Err(CoreError::Validation(
            "failed or cancelled Machine Run-current retains nonterminal reducer state".to_owned(),
        ));
    }
    Ok(())
}

/// Closed unbounded index selected by a command-shaped read.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(tag = "index", rename_all = "snake_case", deny_unknown_fields)]
pub enum MachineRunIndexSelector {
    /// Effects requiring governance.
    GovernanceEffects,
    /// Effects with an unknown world outcome.
    UnknownEffects,
    /// Effects not yet settled.
    PendingEffects,
    /// Effects which a Run terminal transition must update.
    TerminalTransitionEffects,
    /// Open scopes.
    OpenScopes,
    /// Unresolved blocking obligations.
    UnresolvedObligations,
    /// All Effects owned by one open scope.
    ScopeEffects {
        /// Owning scope identity.
        scope_id: String,
    },
    /// Mutating Effects owned by one open scope.
    ScopeMutatingEffects {
        /// Owning scope identity.
        scope_id: String,
    },
    /// Pre-release Effects cancelled by aborting one scope.
    ScopeAbortTransitions {
        /// Owning scope identity.
        scope_id: String,
    },
    /// Released mutating Effects which block aborting one scope.
    ScopeAbortBlockers {
        /// Owning scope identity.
        scope_id: String,
    },
}

impl MachineRunIndexSelector {
    fn verify(&self) -> Result<()> {
        match self {
            Self::ScopeEffects { scope_id }
            | Self::ScopeMutatingEffects { scope_id }
            | Self::ScopeAbortTransitions { scope_id }
            | Self::ScopeAbortBlockers { scope_id } => validate_identity("Machine scope", scope_id),
            Self::GovernanceEffects
            | Self::UnknownEffects
            | Self::PendingEffects
            | Self::TerminalTransitionEffects
            | Self::OpenScopes
            | Self::UnresolvedObligations => Ok(()),
        }
    }

    fn validate_entries(&self, entries: &[String]) -> Result<()> {
        for entry in entries {
            match self {
                Self::OpenScopes => validate_identity("Machine open Scope", entry)?,
                Self::GovernanceEffects
                | Self::UnknownEffects
                | Self::PendingEffects
                | Self::TerminalTransitionEffects
                | Self::ScopeEffects { .. }
                | Self::ScopeMutatingEffects { .. }
                | Self::ScopeAbortTransitions { .. }
                | Self::ScopeAbortBlockers { .. } => {
                    crate::validate_content_id("Machine Effect index entry", entry)?;
                }
                Self::UnresolvedObligations => {
                    crate::validate_content_id("Machine obligation index entry", entry)?;
                }
            }
        }
        Ok(())
    }
}

/// Derive the unique typed value identity for one reducer-index membership.
///
/// # Errors
///
/// Returns an error when the Run, selector, or entry identity is invalid or
/// cannot be serialized canonically.
#[doc(hidden)]
pub fn machine_index_membership_value_id(
    run_id: &str,
    selector: &MachineRunIndexSelector,
    entry: &str,
) -> Result<String> {
    validate_identity("Machine Run", run_id)?;
    selector.verify()?;
    selector.validate_entries(&[entry.to_owned()])?;
    content_id(
        MACHINE_INDEX_MEMBERSHIP_VALUE_VERSION,
        &(run_id, selector, entry),
    )
}

/// One bounded exact page resolved under a pinned physical index root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineRunIndexPage {
    /// Owning Run.
    run_id: String,
    /// Selected reducer index.
    selector: MachineRunIndexSelector,
    /// Lower-authority verified omission-free page.
    page: VerifiedMapPage,
    /// Identities in the Store's authenticated key-hash traversal order.
    entries: Vec<String>,
}

#[derive(serde::Serialize)]
struct MachineRunIndexPageBudget<'a> {
    run_id: &'a str,
    selector: &'a MachineRunIndexSelector,
    source: &'a MachineMapRoot,
    cursor: Option<&'a str>,
    next_cursor: Option<&'a str>,
    entries: &'a [String],
}

impl MachineRunIndexPage {
    /// Verify one raw lower proof against the exact selected source root.
    #[doc(hidden)]
    pub fn verify_proof(
        run_id: String,
        selector: MachineRunIndexSelector,
        source: &MachineMapRoot,
        after: Option<&MapPosition>,
        proof: &MapRangeProof,
    ) -> Result<Self> {
        validate_identity("Machine Run", &run_id)?;
        selector.verify()?;
        let page = verify_map_range(
            source,
            after,
            MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
            cymule_authenticated_collections::MAX_PAGE_BYTES,
            proof,
        )?;
        let entries = page
            .entries()
            .iter()
            .map(|(position, _)| position.key().to_owned())
            .collect::<Vec<_>>();
        selector.validate_entries(&entries)?;
        for (position, value) in page.entries() {
            let expected = machine_index_membership_value_id(&run_id, &selector, position.key())?;
            if value != &expected {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine Run-index key {:?} has the wrong typed membership value",
                    position.key()
                )));
            }
        }
        let page = Self {
            run_id,
            selector,
            page,
            entries,
        };
        page.verify_local()?;
        Ok(page)
    }

    fn verify_local(&self) -> Result<()> {
        validate_identity("Machine Run", &self.run_id)?;
        self.selector.verify()?;
        if self.entries.len() > MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES {
            return Err(CoreError::Validation(
                "Machine Run-index page exceeds its fixed entry bound".to_owned(),
            ));
        }
        let unique = self.entries.iter().collect::<BTreeSet<_>>();
        if unique.len() != self.entries.len() {
            return Err(CoreError::Validation(
                "Machine Run-index page repeats an identity".to_owned(),
            ));
        }
        self.selector.validate_entries(&self.entries)?;
        let verified_entries = self
            .page
            .entries()
            .iter()
            .map(|(position, _)| position.key())
            .collect::<Vec<_>>();
        if verified_entries != self.entries.iter().map(String::as_str).collect::<Vec<_>>() {
            return Err(CoreError::IdentityMismatch(
                "Machine Run-index page changed its verified entries".to_owned(),
            ));
        }
        Ok(())
    }

    /// Exact index selected by this page.
    pub const fn selector(&self) -> &MachineRunIndexSelector {
        &self.selector
    }

    /// Exact source root under which the durable resolver produced this page.
    pub fn source(&self) -> &MachineMapRoot {
        self.page.root()
    }

    /// Entries in authenticated Store traversal order.
    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    /// Opaque input cursor consumed by the durable resolver.
    pub fn cursor(&self) -> Option<&str> {
        self.page.after().map(MapPosition::key)
    }

    /// Opaque successor cursor, absent on the terminal page.
    pub fn next_cursor(&self) -> Option<&str> {
        self.page.next_position().map(MapPosition::key)
    }

    fn budget(&self) -> MachineRunIndexPageBudget<'_> {
        MachineRunIndexPageBudget {
            run_id: &self.run_id,
            selector: &self.selector,
            source: self.source(),
            cursor: self.cursor(),
            next_cursor: self.next_cursor(),
            entries: &self.entries,
        }
    }
}

/// Closed proposal-order log selected by bounded queries or paged transitions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(tag = "log", rename_all = "snake_case", deny_unknown_fields)]
pub enum MachineRunLogSelector {
    /// All Scopes in creation order.
    Scopes,
    /// All Effects in proposal order.
    Effects,
    /// All obligations in creation order.
    Obligations,
    /// All Attempts in creation order.
    Attempts,
    /// Run Plan lineage.
    Plans,
    /// Run binding lineage.
    Bindings,
    /// All Effects in one Scope's proposal order.
    ScopeEffects {
        /// Owning Scope identity.
        scope_id: String,
    },
    /// Mutating Effects in one Scope's proposal order.
    ScopeMutatingEffects {
        /// Owning Scope identity.
        scope_id: String,
    },
}

impl MachineRunLogSelector {
    fn verify(&self) -> Result<()> {
        match self {
            Self::ScopeEffects { scope_id } | Self::ScopeMutatingEffects { scope_id } => {
                validate_identity("Machine Scope", scope_id)
            }
            Self::Scopes
            | Self::Effects
            | Self::Obligations
            | Self::Attempts
            | Self::Plans
            | Self::Bindings => Ok(()),
        }
    }

    fn validate_entries(&self, entries: &[String]) -> Result<()> {
        for entry in entries {
            match self {
                Self::Scopes => validate_identity("Machine Scope log entry", entry)?,
                Self::Effects
                | Self::Obligations
                | Self::Attempts
                | Self::Plans
                | Self::Bindings
                | Self::ScopeEffects { .. }
                | Self::ScopeMutatingEffects { .. } => {
                    crate::validate_content_id("Machine ordered log entry", entry)?;
                }
            }
        }
        Ok(())
    }
}

/// Derive the unique typed value identity for one proposal-order log entry.
///
/// # Errors
///
/// Returns an error when the Run, selector, or entry identity is invalid or
/// cannot be serialized canonically.
#[doc(hidden)]
pub fn machine_order_entry_value_id(
    run_id: &str,
    selector: &MachineRunLogSelector,
    entry: &str,
) -> Result<String> {
    validate_identity("Machine Run", run_id)?;
    selector.verify()?;
    selector.validate_entries(&[entry.to_owned()])?;
    content_id(
        MACHINE_ORDER_ENTRY_VALUE_VERSION,
        &(run_id, selector, entry),
    )
}

/// One exact contiguous page from a pinned proposal-order log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineRunLogPage {
    run_id: String,
    selector: MachineRunLogSelector,
    range: VerifiedLogRange,
    entries: Vec<String>,
}

#[derive(serde::Serialize)]
struct MachineRunLogPageBudget<'a> {
    run_id: &'a str,
    selector: &'a MachineRunLogSelector,
    source: &'a MachineLogRoot,
    start: u64,
    entries: &'a [String],
}

impl MachineRunLogPage {
    /// Verify one raw lower proof against the exact selected source root.
    #[doc(hidden)]
    pub fn verify_proof(
        run_id: String,
        selector: MachineRunLogSelector,
        source: &MachineLogRoot,
        start: u64,
        entries: Vec<String>,
        proof: &LogRangeProof,
    ) -> Result<Self> {
        validate_identity("Machine Run", &run_id)?;
        selector.verify()?;
        let range = verify_log_range(
            source,
            start,
            MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
            cymule_authenticated_collections::MAX_PAGE_BYTES,
            proof,
        )?;
        if entries.len() != range.values().len() {
            return Err(CoreError::IdentityMismatch(
                "Machine Run-log page value count differs from its verified range".to_owned(),
            ));
        }
        selector.validate_entries(&entries)?;
        for (entry, value_id) in entries.iter().zip(range.values()) {
            let expected = machine_order_entry_value_id(&run_id, &selector, entry)?;
            if value_id != &expected {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine Run-log entry {entry:?} has the wrong typed value identity"
                )));
            }
        }
        let page = Self {
            run_id,
            selector,
            range,
            entries,
        };
        page.verify_local()?;
        Ok(page)
    }

    fn verify_local(&self) -> Result<()> {
        validate_identity("Machine Run", &self.run_id)?;
        self.selector.verify()?;
        if self.entries.len() > MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES {
            return Err(CoreError::Validation(
                "Machine Run-log page exceeds its fixed entry bound".to_owned(),
            ));
        }
        let requires_unique_entries = !matches!(
            self.selector,
            MachineRunLogSelector::Plans | MachineRunLogSelector::Bindings
        );
        let unique = self.entries.iter().collect::<BTreeSet<_>>();
        if requires_unique_entries && unique.len() != self.entries.len() {
            return Err(CoreError::Validation(
                "Machine Run-log page repeats an identity".to_owned(),
            ));
        }
        self.selector.validate_entries(&self.entries)?;
        Ok(())
    }

    /// Exact selector.
    pub const fn selector(&self) -> &MachineRunLogSelector {
        &self.selector
    }

    /// Exact source log.
    pub fn source(&self) -> &MachineLogRoot {
        self.range.root()
    }

    /// Zero-based first logical entry.
    pub const fn start(&self) -> u64 {
        self.range.start()
    }

    /// Exact entries in proposal order.
    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    /// First unprocessed logical index.
    ///
    /// # Errors
    ///
    /// Returns a validation error if the bounded range endpoint overflows.
    pub fn end(&self) -> Result<u64> {
        self.range
            .start()
            .checked_add(
                u64::try_from(self.entries.len())
                    .map_err(|error| CoreError::Validation(error.to_string()))?,
            )
            .ok_or_else(|| {
                CoreError::Validation("Machine Run-log page range overflowed".to_owned())
            })
    }

    /// Whether this page reaches the exact source end.
    ///
    /// # Errors
    ///
    /// This accessor is currently infallible after page construction; the
    /// result shape is retained with the verified page API.
    pub fn is_terminal(&self) -> Result<bool> {
        Ok(!self.range.has_more())
    }

    fn budget(&self) -> MachineRunLogPageBudget<'_> {
        MachineRunLogPageBudget {
            run_id: &self.run_id,
            selector: &self.selector,
            source: self.source(),
            start: self.start(),
            entries: &self.entries,
        }
    }
}

/// Multi-page semantic action retained under the exclusive Run fence.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum MachinePagedTransitionAction {
    /// Commit one Scope's mutating Effects into obligations.
    CommitScope {
        /// Scope whose proposal-ordered mutations are committed.
        scope_id: String,
    },
    /// Abort one Scope and terminalize every pre-release Effect.
    AbortScope {
        /// Scope whose pre-release Effects are discarded.
        scope_id: String,
    },
    /// Fail one Run after terminalizing Effects and closing Scopes.
    FailRun,
    /// Cancel one Run after terminalizing Effects and closing Scopes.
    CancelRun,
}

impl MachinePagedTransitionAction {
    fn verify(&self) -> Result<()> {
        match self {
            Self::CommitScope { scope_id } | Self::AbortScope { scope_id } => {
                validate_identity("Machine paged Scope", scope_id)
            }
            Self::FailRun | Self::CancelRun => Ok(()),
        }
    }
}

/// Current page family of one persisted transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MachinePagedTransitionPhase {
    /// Process proposal-ordered Effects.
    Effects,
    /// Process creation-ordered Scopes.
    Scopes,
    /// Every source page was consumed; one final CAS may publish the result.
    Finalize,
}

/// Physical shadow roots mutated out of view until the final atomic swap.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachinePagedShadowRoots {
    /// Shadow Run child maps.
    pub children: MachineRunChildRoots,
    /// Shadow proposal-order logs.
    pub order: MachineRunOrderRoots,
    /// Shadow Run reducer indexes.
    pub indexes: MachineRunIndexRoots,
}

impl MachinePagedShadowRoots {
    fn verify(&self) -> Result<()> {
        self.children.verify()?;
        self.order.verify()?;
        self.indexes.verify()
    }
}

/// Frozen, bounded single-command batch manifest retained through every page.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachinePagedBatchManifest {
    /// Exact batch generation.
    pub batch_version: String,
    /// Original parent-bound batch identity.
    pub batch_id: String,
    /// Source semantic authority at reservation time.
    pub parent_authority_root: String,
    /// The sole command and its exact intent/envelope hashes.
    pub member: MachineCommandBatchMember,
    /// Exact proposed material digest, null when no material was proposed.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub material_digest: Option<String>,
    /// Complete proposed material preimage, null exactly with its digest.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub material_source: Option<MachineCommandBatchMaterialSource>,
    /// Canonically ordered complete batch Plan membership.
    pub plan_ids: Vec<String>,
    /// Canonically ordered complete batch Artifact membership.
    pub artifacts: Vec<ArtifactRef>,
}

impl MachinePagedBatchManifest {
    fn from_command(
        frontier: &MachineAuthorityFrontier,
        envelope: &CommandEnvelope,
    ) -> Result<Self> {
        let (plan_ids, artifacts) = command_material_membership(&envelope.command)?;
        let member = MachineCommandBatchMember {
            position: 0,
            command_id: envelope.command_id.clone(),
            intent_hash: command_intent_hash(envelope)?,
            semantic_hash: canonical_digest(envelope)?,
        };
        let batch_id = machine_command_batch_id(
            &frontier.authority_root,
            std::slice::from_ref(&member),
            None,
            None,
            &plan_ids,
            &artifacts,
        )?;
        Ok(Self {
            batch_version: MACHINE_COMMAND_BATCH_VERSION.to_owned(),
            batch_id,
            parent_authority_root: frontier.authority_root.clone(),
            member,
            material_digest: None,
            material_source: None,
            plan_ids,
            artifacts,
        })
    }

    fn verify(&self, envelope: &CommandEnvelope) -> Result<()> {
        let (required_plans, required_artifacts) = command_material_membership(&envelope.command)?;
        if self.batch_version != MACHINE_COMMAND_BATCH_VERSION
            || self.member.position != 0
            || self.member.command_id != envelope.command_id
            || self.member.intent_hash != command_intent_hash(envelope)?
            || self.member.semantic_hash != canonical_digest(envelope)?
            || self.plan_ids.len() > MAX_MACHINE_MATERIAL_PLANS + 2
            || self.artifacts.len() > MAX_MACHINE_MATERIAL_ARTIFACTS + 2
            || !self.plan_ids.windows(2).all(|pair| pair[0] < pair[1])
            || !self
                .artifacts
                .windows(2)
                .all(|pair| pair[0].artifact_id < pair[1].artifact_id)
            || required_plans.iter().any(|id| !self.plan_ids.contains(id))
            || required_artifacts
                .iter()
                .any(|reference| !self.artifacts.contains(reference))
        {
            return Err(CoreError::IdentityMismatch(
                "paged command changed its frozen batch manifest".to_owned(),
            ));
        }
        if self.material_digest.is_some() != self.material_source.is_some() {
            return Err(CoreError::IdentityMismatch(
                "paged material source and digest nullability differ".to_owned(),
            ));
        }
        if let Some(source) = &self.material_source {
            source.verify()?;
            if source.plan_ids.iter().any(|id| !self.plan_ids.contains(id))
                || source
                    .artifacts
                    .iter()
                    .any(|reference| !self.artifacts.contains(reference))
            {
                return Err(CoreError::IdentityMismatch(
                    "paged material source is outside the exact batch".to_owned(),
                ));
            }
        }
        if let Some(digest) = &self.material_digest {
            crate::validate_content_id("paged batch material", digest)?;
        } else if self.plan_ids != required_plans || self.artifacts != required_artifacts {
            return Err(CoreError::IdentityMismatch(
                "material-free paged batch carries extra members".to_owned(),
            ));
        }
        for id in &self.plan_ids {
            crate::validate_content_id("paged batch Plan", id)?;
        }
        for reference in &self.artifacts {
            reference.validate()?;
        }
        if self.batch_id
            != machine_command_batch_id(
                &self.parent_authority_root,
                std::slice::from_ref(&self.member),
                self.material_digest.as_deref(),
                self.material_source.as_ref(),
                &self.plan_ids,
                &self.artifacts,
            )?
        {
            return Err(CoreError::IdentityMismatch(
                "paged batch identity changed".to_owned(),
            ));
        }
        Ok(())
    }

    fn record(
        &self,
        admission_parent: &str,
        receipt: CommandReceipt,
        result: &str,
    ) -> Result<MachineCommandBatchRecord> {
        let mut record = MachineCommandBatchRecord {
            batch_version: self.batch_version.clone(),
            batch_id: self.batch_id.clone(),
            parent_authority_root: self.parent_authority_root.clone(),
            admission_parent_authority_root: admission_parent.to_owned(),
            members: vec![self.member.clone()],
            material_digest: self.material_digest.clone(),
            material_source: self.material_source.clone(),
            plan_ids: self.plan_ids.clone(),
            artifacts: self.artifacts.clone(),
            event_ids: receipt.event_ids.clone(),
            receipts: vec![receipt],
            result_authority_root: result.to_owned(),
            batch_receipt_id: String::new(),
        };
        record.batch_receipt_id = record.expected_receipt_id()?;
        record.verify()?;
        Ok(record)
    }
}

/// Authenticated proposal-only material maps owned by a pending transition.
/// Their bytes are GC-reachable but are not global semantic admissions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachinePagedMaterialRoots {
    /// Exact staged Plan records keyed by semantic Plan identity.
    pub plans: MachineMapRoot,
    /// Exact staged Artifact records keyed by semantic Artifact identity.
    pub artifacts: MachineMapRoot,
}

impl MachinePagedMaterialRoots {
    fn empty() -> Self {
        Self {
            plans: MachineMapRoot::empty(),
            artifacts: MachineMapRoot::empty(),
        }
    }

    fn verify(&self, has_material: bool) -> Result<()> {
        self.plans.verify()?;
        self.artifacts.verify()?;
        if self.plans.entries > MAX_MACHINE_MATERIAL_PLANS as u64
            || self.artifacts.entries > MAX_MACHINE_MATERIAL_ARTIFACTS as u64
            || has_material != (self.plans.entries != 0 || self.artifacts.entries != 0)
        {
            return Err(CoreError::IdentityMismatch(
                "paged staged material roots do not match their manifest".to_owned(),
            ));
        }
        Ok(())
    }
}

/// O(1) persisted state of one revision-pinned K-page transition.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachinePagedTransitionCurrent {
    /// Current schema generation.
    pub transition_version: String,
    /// Stable identity of the immutable command, Run, source, and parent.
    pub transition_id: String,
    /// Complete bounded command retained for autonomous crash recovery.
    pub envelope: CommandEnvelope,
    /// Original complete batch authority; never reconstructed at finalization.
    pub batch_manifest: MachinePagedBatchManifest,
    /// Independently rooted proposed material, hidden from canonical catalogs.
    pub staged_material: MachinePagedMaterialRoots,
    /// Original external command identity.
    pub command_id: String,
    /// Canonical digest of the exact original command envelope.
    pub command_hash: String,
    /// Owning Run.
    pub run_id: String,
    /// Durable parent revision pinned when the transition began.
    pub parent_revision: String,
    /// Exact digest of the source Ready Run current before its fence is added.
    pub source_run_current_digest: String,
    /// Closed terminal action.
    pub action: MachinePagedTransitionAction,
    /// Digest of the full action payload retained by the durable pending leaf.
    pub target_action_digest: String,
    /// Current source-log family.
    pub phase: MachinePagedTransitionPhase,
    /// Immutable Effect proposal-order source.
    pub effect_source: MachineLogRoot,
    /// Immutable Scope creation-order source.
    pub scope_source: MachineLogRoot,
    /// Exact next logical index in the current source.
    pub next_index: u64,
    /// Total values processed across all phases.
    pub processed_count: u64,
    /// Core-owned rolling commitment over processed typed results.
    pub processed_commitment: String,
    /// Obligation count accumulated for Scope commit.
    pub obligation_count: u64,
    /// Proposal-order obligation commitment accumulated for Scope commit.
    pub obligation_commitment: String,
    /// Out-of-view physical result roots.
    pub shadow: MachinePagedShadowRoots,
}

impl MachinePagedTransitionCurrent {
    /// Verify fixed-size persisted transition authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the transition identity, source cursor, result
    /// roots, or action-specific accumulator is inconsistent.
    pub fn verify(&self) -> Result<()> {
        self.verify_identity_binding()?;
        if let Some(source) = &self.batch_manifest.material_source
            && (u64::try_from(source.plan_ids.len()).ok()
                != Some(self.staged_material.plans.entries)
                || u64::try_from(source.artifacts.len()).ok()
                    != Some(self.staged_material.artifacts.entries))
        {
            return Err(CoreError::IdentityMismatch(
                "paged source material count differs from its staging roots".to_owned(),
            ));
        }
        self.verify_cursor_authority()?;
        self.verify_action_accumulator()
    }

    fn verify_identity_binding(&self) -> Result<()> {
        if self.transition_version != MACHINE_PAGED_TRANSITION_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported Machine paged-transition version {:?}",
                self.transition_version
            )));
        }
        for (kind, value) in [
            ("Machine paged transition", self.transition_id.as_str()),
            (
                "Machine paged parent revision",
                self.parent_revision.as_str(),
            ),
            (
                "Machine paged target action digest",
                self.target_action_digest.as_str(),
            ),
            (
                "Machine paged processed commitment",
                self.processed_commitment.as_str(),
            ),
            (
                "Machine paged obligation commitment",
                self.obligation_commitment.as_str(),
            ),
        ] {
            crate::validate_content_id(kind, value)?;
        }
        validate_identity("Machine paged command", &self.command_id)?;
        validate_identity("Machine paged Run", &self.run_id)?;
        validate_envelope(&self.envelope)?;
        self.batch_manifest.verify(&self.envelope)?;
        self.staged_material
            .verify(self.batch_manifest.material_digest.is_some())?;
        if !is_canonical_digest(&self.command_hash)
            || !is_canonical_digest(&self.source_run_current_digest)
        {
            return Err(CoreError::Validation(
                "Machine paged command or source Run hash is not a canonical digest".to_owned(),
            ));
        }
        self.action.verify()?;
        if self.envelope.command_id != self.command_id
            || self.envelope.run_id != self.run_id
            || canonical_digest(&self.envelope)? != self.command_hash
            || content_id(MACHINE_PAGED_ACTION_ID_DOMAIN, &self.envelope.command)?
                != self.target_action_digest
            || !paged_action_matches_command(&self.action, &self.envelope.command)
            || self.transition_id != self.expected_transition_id()?
        {
            return Err(CoreError::IdentityMismatch(
                "Machine paged transition does not bind its complete command and source".to_owned(),
            ));
        }
        self.effect_source.verify()?;
        self.scope_source.verify()?;
        self.shadow.verify()?;
        Ok(())
    }

    fn verify_cursor_authority(&self) -> Result<()> {
        for count in [self.next_index, self.processed_count, self.obligation_count] {
            if count > crate::MAX_EXACT_INTEGER {
                return Err(CoreError::Validation(
                    "Machine paged-transition count exceeds the exact range".to_owned(),
                ));
            }
        }
        let source_len = match self.phase {
            MachinePagedTransitionPhase::Effects => self.effect_source.len,
            MachinePagedTransitionPhase::Scopes => self.scope_source.len,
            MachinePagedTransitionPhase::Finalize => 0,
        };
        if (self.phase == MachinePagedTransitionPhase::Finalize && self.next_index != 0)
            || (self.phase != MachinePagedTransitionPhase::Finalize && self.next_index > source_len)
        {
            return Err(CoreError::Validation(
                "Machine paged-transition cursor is outside its source".to_owned(),
            ));
        }
        let processed_floor = match self.phase {
            MachinePagedTransitionPhase::Effects => self.next_index,
            MachinePagedTransitionPhase::Scopes => self
                .effect_source
                .len
                .checked_add(self.next_index)
                .ok_or_else(|| {
                    CoreError::Validation(
                        "Machine paged-transition processed range overflowed".to_owned(),
                    )
                })?,
            MachinePagedTransitionPhase::Finalize => self
                .effect_source
                .len
                .checked_add(self.scope_source.len)
                .ok_or_else(|| {
                    CoreError::Validation(
                        "Machine paged-transition processed range overflowed".to_owned(),
                    )
                })?,
        };
        if self.processed_count != processed_floor {
            return Err(CoreError::Validation(
                "Machine paged-transition count does not match its exact cursor".to_owned(),
            ));
        }
        let empty_commitment = lineage_genesis(MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN)?;
        if self.processed_count == 0 && self.processed_commitment != empty_commitment {
            return Err(CoreError::Validation(
                "empty Machine paged transition has a non-genesis commitment".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_action_accumulator(&self) -> Result<()> {
        let empty_obligations = crate::machine::scope_obligation_commitment_genesis()?;
        match &self.action {
            MachinePagedTransitionAction::CommitScope { .. } => {
                if self.scope_source.len != 0
                    || self.obligation_count > self.processed_count
                    || (self.obligation_count == 0
                        && self.obligation_commitment != empty_obligations)
                {
                    return Err(CoreError::Validation(
                        "Scope commit paged state has an inexact obligation accumulator".to_owned(),
                    ));
                }
            }
            MachinePagedTransitionAction::AbortScope { .. } => {
                if self.scope_source.len != 0
                    || self.obligation_count != 0
                    || self.obligation_commitment != empty_obligations
                {
                    return Err(CoreError::Validation(
                        "Scope abort paged state carried unrelated scope or obligation work"
                            .to_owned(),
                    ));
                }
            }
            MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun => {
                if self.obligation_count != 0 || self.obligation_commitment != empty_obligations {
                    return Err(CoreError::Validation(
                        "Run terminal paged state carried Scope-commit obligations".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn expected_transition_id(&self) -> Result<String> {
        content_id(
            MACHINE_PAGED_TRANSITION_VERSION,
            &(
                self.command_id.as_str(),
                self.command_hash.as_str(),
                self.run_id.as_str(),
                self.parent_revision.as_str(),
                self.source_run_current_digest.as_str(),
                &self.action,
                self.target_action_digest.as_str(),
                &self.effect_source,
                &self.scope_source,
                &self.batch_manifest,
                &self.staged_material,
            ),
        )
    }

    /// Verify the whole original batch request before resuming pending work.
    ///
    /// # Errors
    ///
    /// Returns an error for partial/extra commands, different material, actor,
    /// command content, or parent/derived precondition authority.
    pub fn verify_batch_replay(
        &self,
        commands: &[MachinePinnedBatchCommand],
        material_digest: Option<&str>,
    ) -> Result<()> {
        self.verify()?;
        let [command] = commands else {
            return Err(CoreError::IdentityMismatch(
                "pending paged replay requires its sole original batch command".to_owned(),
            ));
        };
        if !matches!(
            command.precondition,
            MachinePinnedBatchPrecondition::Parent(_)
        ) || command.envelope(None)? != self.envelope
            || material_digest != self.batch_manifest.material_digest.as_deref()
            || command.intent_hash()? != self.batch_manifest.member.intent_hash
        {
            return Err(CoreError::IdentityMismatch(
                "pending paged replay changed its batch request".to_owned(),
            ));
        }
        Ok(())
    }
}

fn paged_action_matches_command(action: &MachinePagedTransitionAction, command: &Command) -> bool {
    matches!(
        (action, command),
        (
            MachinePagedTransitionAction::CommitScope { scope_id: left },
            Command::CommitScope { scope_id: right }
        ) if left == right
    ) || matches!(
        (action, command),
        (
            MachinePagedTransitionAction::AbortScope { scope_id: left },
            Command::AbortScope { scope_id: right }
        ) if left == right
    ) || matches!(
        (action, command),
        (
            MachinePagedTransitionAction::FailRun,
            Command::FailRun { .. }
        ) | (
            MachinePagedTransitionAction::CancelRun,
            Command::CancelRun { .. }
        )
    )
}

/// Exact typed leaves for one persisted transition page.
///
/// The durable resolver constructs this non-wire view from the transition's
/// immutable source log and current shadow roots. Missing keys are invariant
/// failures; extra keys are rejected.
#[derive(Debug, Clone, PartialEq)]
pub struct MachinePagedReadInputs {
    live_run: MachineRunCurrent,
    page: MachineRunLogPage,
    scopes: BTreeMap<String, MachineScopeCurrent>,
    effects: BTreeMap<String, crate::EffectProjection>,
    obligations: BTreeMap<String, Option<ObligationProjection>>,
}

impl MachinePagedReadInputs {
    /// Assemble one resolver-owned transition page.
    #[doc(hidden)]
    pub fn new(
        live_run: MachineRunCurrent,
        page: MachineRunLogPage,
        scopes: BTreeMap<String, MachineScopeCurrent>,
        effects: BTreeMap<String, crate::EffectProjection>,
        obligations: BTreeMap<String, Option<ObligationProjection>>,
    ) -> Self {
        Self {
            live_run,
            page,
            scopes,
            effects,
            obligations,
        }
    }
}

/// Result of one bounded shadow page and persisted-cursor advance.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedMachinePagedProgress {
    /// Frontier with the replaced transition-map root.
    pub frontier: MachineAuthorityFrontier,
    /// Exact next persisted transition state.
    pub transition: MachinePagedTransitionCurrent,
    /// Scope leaves replaced in the shadow Scope map.
    pub scopes: BTreeMap<String, MachineScopeCurrent>,
    /// Effect leaves replaced in the shadow Effect map.
    pub effects: BTreeMap<String, crate::EffectProjection>,
    /// Obligation leaves inserted or replaced in the shadow obligation map.
    pub obligations: BTreeMap<String, ObligationProjection>,
    /// Exact shadow reducer-index membership changes.
    pub indexes: Vec<MachineRunIndexMembershipDelta>,
    /// Exact shadow proposal-log appends.
    pub logs: Vec<MachineRunLogAppendDelta>,
}

/// Prepared shadow-root page before Store roots are known.
pub struct PreparedPinnedPagedStep {
    frontier: MachineAuthorityFrontier,
    parent_transition: MachinePagedTransitionCurrent,
    transition: MachinePagedTransitionCurrent,
    shadow_run: MachineRunCurrent,
    reduction: PinnedRunReduction,
    plans: Vec<MachinePreparedRootMutation>,
    local_authority: String,
}

/// Prepared cursor persistence after every shadow child root is final.
pub struct PreparedPinnedPagedProgress {
    inner: PreparedPinnedPagedStep,
    transition_plan: MachinePreparedRootMutation,
}

impl PreparedPinnedPagedStep {
    /// Exact shadow child/index/log mutations for this one bounded page.
    ///
    /// # Errors
    ///
    /// Returns an error if this locally prepared page has been altered.
    pub fn shadow_root_mutations(&self) -> Result<&[MachinePreparedRootMutation]> {
        self.verify_local_authority()?;
        Ok(&self.plans)
    }

    /// Bind shadow roots before the transition leaf itself is replaced.
    ///
    /// # Errors
    ///
    /// Returns an error unless every supplied physical root is the exact result
    /// of every requested shadow mutation.
    pub fn finish_shadow_roots(
        mut self,
        updates: Vec<MachineRunRootUpdate>,
    ) -> Result<PreparedPinnedPagedProgress> {
        self.verify_local_authority()?;
        let supplied = consume_bound_root_updates(&self.plans, updates)?;
        for (target, root) in supplied {
            match &target {
                MachineRunRootUpdateTarget::Scopes => {
                    self.shadow_run.children.scopes = require_map_root(&target, &root)?.clone();
                }
                MachineRunRootUpdateTarget::Effects => {
                    self.shadow_run.children.effects = require_map_root(&target, &root)?.clone();
                }
                MachineRunRootUpdateTarget::Obligations => {
                    self.shadow_run.children.obligations =
                        require_map_root(&target, &root)?.clone();
                }
                MachineRunRootUpdateTarget::Index(selector) => apply_result_index_root(
                    &mut self.shadow_run,
                    &mut self.reduction.scopes,
                    selector,
                    require_map_root(&target, &root)?.clone(),
                )?,
                MachineRunRootUpdateTarget::Log(selector) => apply_result_log_root(
                    &mut self.shadow_run,
                    &mut self.reduction.scopes,
                    selector,
                    require_log_root(&target, &root)?.clone(),
                )?,
                _ => {
                    return Err(CoreError::IdentityMismatch(
                        "paged shadow stage returned a non-shadow root".to_owned(),
                    ));
                }
            }
        }
        self.transition.shadow.children = self.shadow_run.children.clone();
        self.transition.shadow.order = self.shadow_run.order.clone();
        self.transition.shadow.indexes = self.shadow_run.indexes.clone();
        self.transition.verify()?;
        let transition_plan = prepared_root_mutation(
            MachineRunRootUpdateTarget::PagedTransitions,
            MachinePhysicalRoot::Map(self.frontier.paged_transitions.clone()),
            self.frontier.paged_transitions.entries,
            MachineTypedRootMutation::PutPagedTransition(Box::new(self.transition.clone())),
        )?;
        self.refresh_local_authority()?;
        Ok(PreparedPinnedPagedProgress {
            inner: self,
            transition_plan,
        })
    }

    fn refresh_local_authority(&mut self) -> Result<()> {
        self.local_authority = canonical_digest(&(
            PREPARED_PAGED_STEP_AUTHORITY_DOMAIN,
            &self.frontier,
            &self.parent_transition,
            &self.transition,
            &self.shadow_run,
            &self.reduction.scopes,
            &self.reduction.effects,
            &self.reduction.obligations,
            &self.reduction.indexes,
            &self.reduction.logs,
            &self.plans,
        ))?;
        Ok(())
    }

    fn verify_local_authority(&self) -> Result<()> {
        let expected = canonical_digest(&(
            PREPARED_PAGED_STEP_AUTHORITY_DOMAIN,
            &self.frontier,
            &self.parent_transition,
            &self.transition,
            &self.shadow_run,
            &self.reduction.scopes,
            &self.reduction.effects,
            &self.reduction.obligations,
            &self.reduction.indexes,
            &self.reduction.logs,
            &self.plans,
        ))?;
        if expected != self.local_authority {
            return Err(CoreError::IdentityMismatch(
                "prepared paged Machine step lost local authority".to_owned(),
            ));
        }
        Ok(())
    }
}

impl PreparedPinnedPagedProgress {
    /// Exact transition-map replacement after shadow roots are final.
    ///
    /// # Errors
    ///
    /// Returns an error if the prepared page lost local authority.
    pub fn transition_root_mutation(&self) -> Result<&MachinePreparedRootMutation> {
        self.inner.verify_local_authority()?;
        Ok(&self.transition_plan)
    }

    /// Bind the transition-map root and publish one durable page of progress.
    ///
    /// # Errors
    ///
    /// Returns an error unless the supplied update is the exact requested
    /// transition-map replacement and the result frontier remains valid.
    pub fn finish(mut self, update: MachineRunRootUpdate) -> Result<PinnedMachinePagedProgress> {
        self.inner.verify_local_authority()?;
        let supplied = consume_bound_root_updates(&[self.transition_plan], vec![update])?;
        self.inner.frontier.paged_transitions = require_map_root(
            &MachineRunRootUpdateTarget::PagedTransitions,
            supplied
                .get(&MachineRunRootUpdateTarget::PagedTransitions)
                .ok_or_else(|| {
                    CoreError::IdentityMismatch(
                        "paged transition root result is missing".to_owned(),
                    )
                })?,
        )?
        .clone();
        self.inner.frontier.verify()?;
        Ok(PinnedMachinePagedProgress {
            frontier: self.inner.frontier,
            transition: self.inner.transition,
            scopes: self.inner.reduction.scopes,
            effects: self.inner.reduction.effects,
            obligations: self.inner.reduction.obligations,
            indexes: self.inner.reduction.indexes,
            logs: self.inner.reduction.logs,
        })
    }
}

/// Exact final reads for a transition whose source pages are exhausted.
#[derive(Debug, Clone, PartialEq)]
pub struct MachinePagedFinalizeInputs {
    live_run: MachineRunCurrent,
    scopes: BTreeMap<String, MachineScopeCurrent>,
    active_attempt: Option<crate::AttemptProjection>,
    command_index_proof: MachineCommandIndexProof,
    material: Option<(MachineMaterialAdmission, MachineMaterialParentReads)>,
}

impl MachinePagedFinalizeInputs {
    /// Assemble resolver-owned final reads under the current global frontier.
    #[doc(hidden)]
    pub fn new(
        live_run: MachineRunCurrent,
        scopes: BTreeMap<String, MachineScopeCurrent>,
        active_attempt: Option<crate::AttemptProjection>,
        command_index_proof: MachineCommandIndexProof,
        material: Option<(MachineMaterialAdmission, MachineMaterialParentReads)>,
    ) -> Self {
        Self {
            live_run,
            scopes,
            active_attempt,
            command_index_proof,
            material,
        }
    }
}

/// Prepared final shadow leaf/index roots.
pub struct PreparedPinnedPagedFinalize {
    frontier: MachineAuthorityFrontier,
    transition: MachinePagedTransitionCurrent,
    live_run: MachineRunCurrent,
    result_current: MachineRunCurrent,
    receipt: CommandReceipt,
    machine_delta: MachineRootDelta,
    reduction: PinnedRunReduction,
    plans: Vec<MachinePreparedRootMutation>,
    local_authority: String,
}

/// Prepared final global swap after the complete Run current is known.
pub struct PreparedPinnedPagedPublish {
    inner: PreparedPinnedPagedFinalize,
    plans: Vec<MachinePreparedRootMutation>,
}

impl PreparedPinnedPagedFinalize {
    /// Final shadow Scope, Attempt, and index applies.
    ///
    /// # Errors
    ///
    /// Returns an error if the locally prepared finalization has been altered.
    pub fn shadow_root_mutations(&self) -> Result<&[MachinePreparedRootMutation]> {
        self.verify_local_authority()?;
        Ok(&self.plans)
    }

    /// Bind final shadow roots and construct the exact live Run swap.
    ///
    /// # Errors
    ///
    /// Returns an error unless every supplied root exactly applies the requested
    /// final shadow mutations and produces one valid Run current.
    pub fn finish_shadow_roots(
        mut self,
        updates: Vec<MachineRunRootUpdate>,
    ) -> Result<PreparedPinnedPagedPublish> {
        self.verify_local_authority()?;
        let supplied = consume_bound_root_updates(&self.plans, updates)?;
        for (target, root) in supplied {
            match &target {
                MachineRunRootUpdateTarget::Scopes => {
                    self.result_current.children.scopes = require_map_root(&target, &root)?.clone();
                }
                MachineRunRootUpdateTarget::Attempts => {
                    self.result_current.children.attempts =
                        require_map_root(&target, &root)?.clone();
                }
                MachineRunRootUpdateTarget::Index(selector) => apply_result_index_root(
                    &mut self.result_current,
                    &mut self.reduction.scopes,
                    selector,
                    require_map_root(&target, &root)?.clone(),
                )?,
                _ => {
                    return Err(CoreError::IdentityMismatch(
                        "paged final shadow stage returned an unrelated root".to_owned(),
                    ));
                }
            }
        }
        self.result_current.verify()?;
        let transition_digest = canonical_digest(&self.transition)?;
        let plans = vec![
            prepared_root_mutation(
                MachineRunRootUpdateTarget::Runs,
                MachinePhysicalRoot::Map(self.frontier.runs.clone()),
                self.frontier.runs.entries,
                MachineTypedRootMutation::PutRuns(BTreeMap::from([(
                    self.result_current.run_id.clone(),
                    self.result_current.clone(),
                )])),
            )?,
            prepared_root_mutation(
                MachineRunRootUpdateTarget::PendingCommands,
                MachinePhysicalRoot::Map(self.frontier.pending_commands.clone()),
                checked_result_count(self.frontier.pending_commands.entries, 0, 1)?,
                MachineTypedRootMutation::RemoveCommandReservation {
                    command_id: self.transition.command_id.clone(),
                    transition_id: self.transition.transition_id.clone(),
                },
            )?,
            prepared_root_mutation(
                MachineRunRootUpdateTarget::PagedTransitions,
                MachinePhysicalRoot::Map(self.frontier.paged_transitions.clone()),
                checked_result_count(self.frontier.paged_transitions.entries, 0, 1)?,
                MachineTypedRootMutation::RemovePagedTransition {
                    transition_id: self.transition.transition_id.clone(),
                    transition_digest,
                },
            )?,
        ];
        self.refresh_local_authority()?;
        Ok(PreparedPinnedPagedPublish { inner: self, plans })
    }

    fn refresh_local_authority(&mut self) -> Result<()> {
        self.local_authority = canonical_digest(&(
            PREPARED_PAGED_FINALIZE_AUTHORITY_DOMAIN,
            &self.frontier,
            &self.transition,
            &self.live_run,
            &self.result_current,
            &self.receipt,
            &self.machine_delta,
            &self.reduction.scopes,
            &self.reduction.attempts,
            &self.reduction.indexes,
            &self.plans,
        ))?;
        Ok(())
    }

    fn verify_local_authority(&self) -> Result<()> {
        let expected = canonical_digest(&(
            PREPARED_PAGED_FINALIZE_AUTHORITY_DOMAIN,
            &self.frontier,
            &self.transition,
            &self.live_run,
            &self.result_current,
            &self.receipt,
            &self.machine_delta,
            &self.reduction.scopes,
            &self.reduction.attempts,
            &self.reduction.indexes,
            &self.plans,
        ))?;
        if expected != self.local_authority {
            return Err(CoreError::IdentityMismatch(
                "prepared paged Machine finalization lost local authority".to_owned(),
            ));
        }
        Ok(())
    }
}

impl PreparedPinnedPagedPublish {
    /// Atomic live Run swap and pending-reservation removals.
    ///
    /// # Errors
    ///
    /// Returns an error if the prepared final publish has lost local authority.
    pub fn root_mutations(&self) -> Result<&[MachinePreparedRootMutation]> {
        self.inner.verify_local_authority()?;
        Ok(&self.plans)
    }

    /// Bind the final global roots and publish the one Event/admission.
    ///
    /// # Errors
    ///
    /// Returns an error unless all global updates are exact requested results
    /// and the resulting semantic frontier verifies.
    pub fn finish(
        mut self,
        updates: Vec<MachineRunRootUpdate>,
    ) -> Result<PinnedMachineBatchTransition> {
        self.inner.verify_local_authority()?;
        let supplied = consume_bound_root_updates(&self.plans, updates)?;
        for (target, root) in supplied {
            match target {
                MachineRunRootUpdateTarget::Runs => {
                    self.inner.frontier.runs = require_map_root(&target, &root)?.clone();
                }
                MachineRunRootUpdateTarget::PendingCommands => {
                    self.inner.frontier.pending_commands =
                        require_map_root(&target, &root)?.clone();
                }
                MachineRunRootUpdateTarget::PagedTransitions => {
                    self.inner.frontier.paged_transitions =
                        require_map_root(&target, &root)?.clone();
                }
                _ => unreachable!("paged publish accepted a non-global root"),
            }
        }
        self.inner.frontier.verify()?;
        let batch = self
            .inner
            .machine_delta
            .batches
            .get(&self.inner.transition.batch_manifest.batch_id)
            .cloned()
            .ok_or_else(|| {
                CoreError::NotFound("paged finalization lost its original batch receipt".to_owned())
            })?;
        let step = PinnedMachineRootDelta {
            machine: self.inner.machine_delta.clone(),
            run: Some(MachineRunDelta {
                run_id: self.inner.result_current.run_id.clone(),
                parent_current_digest: Some(canonical_digest(&self.inner.live_run)?),
                result_current: self.inner.result_current,
                scopes: self.inner.reduction.scopes,
                effects: BTreeMap::new(),
                obligations: BTreeMap::new(),
                attempts: self.inner.reduction.attempts,
                indexes: self.inner.reduction.indexes,
                logs: Vec::new(),
            }),
            facts: BTreeMap::new(),
        };
        Ok(PinnedMachineBatchTransition {
            batch,
            frontier: self.inner.frontier,
            machine: self.inner.machine_delta,
            steps: vec![step],
        })
    }
}

/// Scalar Scope leaf plus roots for every unbounded membership or ordering.
///
/// Invocation and Region paths are represented by canonical digests here. A
/// command which must interpret either path carries the bounded exact path as a
/// transient resolver witness and Core checks it against these digests.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineScopeCurrent {
    /// Current schema generation.
    pub scope_current_version: String,
    /// Owning Scope identity.
    pub scope_id: String,
    /// Optional direct parent; null only for the synthetic root Scope.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_scope: Option<String>,
    /// Dynamic invocation which opened this Scope.
    pub invocation_id: String,
    /// Canonical digest of the complete entry-rooted invocation path.
    pub invocation_path_digest: String,
    /// Definition containing this Scope body.
    pub definition_id: String,
    /// Canonical digest of the complete lexical Region path.
    pub region_path_digest: String,
    /// Stable scope-operation site; null only for the synthetic root Scope.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub site_id: Option<String>,
    /// Canonical lifecycle state.
    pub status: crate::ScopeStatus,
    /// Total Effects owned by this scope.
    pub effect_count: u64,
    /// Number of directly open child Scopes.
    pub direct_open_child_count: u64,
    /// Core-owned append commitment over all Effect proposals.
    pub effect_lineage_root: String,
    /// All Effect identities.
    pub effects: MachineMapRoot,
    /// Exact proposal-order log for all Effects.
    pub effect_order: MachineLogRoot,
    /// Core-owned append commitment over mutating Effect proposals.
    pub mutating_effect_lineage_root: String,
    /// Mutating Effect identities.
    pub mutating_effects: MachineMapRoot,
    /// Exact proposal-order log for mutating Effects.
    pub mutating_effect_order: MachineLogRoot,
    /// Effects which abort changes to cancelled-before-release.
    pub abort_transitions: MachineMapRoot,
    /// Released mutating Effects which prevent abort.
    pub abort_blockers: MachineMapRoot,
}

impl MachineScopeCurrent {
    /// Verify the complete persisted Scope leaf and all nested physical roots.
    ///
    /// # Errors
    ///
    /// Returns an error when scalar identity/count authority, root/nested Scope
    /// shape, or any child physical collection is invalid.
    pub fn verify(&self) -> Result<()> {
        if self.scope_current_version != MACHINE_SCOPE_CURRENT_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported Machine Scope-current version {:?}",
                self.scope_current_version
            )));
        }
        validate_identity("Machine Scope", &self.scope_id)?;
        crate::validate_content_id("Machine Scope invocation", &self.invocation_id)?;
        if !is_canonical_digest(&self.invocation_path_digest)
            || !is_canonical_digest(&self.region_path_digest)
        {
            return Err(CoreError::Validation(
                "Machine Scope location digests must be lowercase SHA-256 digests".to_owned(),
            ));
        }
        crate::validate_content_id("Machine Scope Effect lineage", &self.effect_lineage_root)?;
        crate::validate_content_id(
            "Machine Scope mutating-Effect lineage",
            &self.mutating_effect_lineage_root,
        )?;
        crate::validate_semantic_id("Machine Scope definition", &self.definition_id)?;
        match (&self.parent_scope, &self.site_id, self.scope_id.as_str()) {
            (None, None, ROOT_SCOPE_ID) => {}
            (Some(parent), Some(site), scope_id) if scope_id != ROOT_SCOPE_ID => {
                validate_identity("Machine parent Scope", parent)?;
                crate::validate_semantic_id("Machine Scope site", site)?;
                if parent == scope_id {
                    return Err(CoreError::Validation(
                        "Machine Scope cannot parent itself".to_owned(),
                    ));
                }
            }
            _ => {
                return Err(CoreError::Validation(
                    "Machine root and nested Scope scalar authority disagree".to_owned(),
                ));
            }
        }
        if self.effect_count > crate::MAX_EXACT_INTEGER
            || self.direct_open_child_count > crate::MAX_EXACT_INTEGER
        {
            return Err(CoreError::Validation(
                "Machine Scope-current scalar count exceeds the exact range".to_owned(),
            ));
        }
        self.effects.verify()?;
        self.effect_order.verify()?;
        self.mutating_effects.verify()?;
        self.mutating_effect_order.verify()?;
        self.abort_transitions.verify()?;
        self.abort_blockers.verify()?;
        if self.effect_count != self.effects.entries
            || self.effect_count != self.effect_order.len
            || self.mutating_effects.entries != self.mutating_effect_order.len
            || self.mutating_effects.entries > self.effect_count
            || self.abort_transitions.entries > self.effect_count
            || self.abort_blockers.entries > self.effect_count
        {
            return Err(CoreError::Validation(
                "Machine Scope-current Effect map, log, or index counts disagree".to_owned(),
            ));
        }
        if self.status != crate::ScopeStatus::Open
            && (self.direct_open_child_count != 0
                || self.abort_transitions.entries != 0
                || self.abort_blockers.entries != 0)
        {
            return Err(CoreError::Validation(
                "closed Machine Scope-current retains open-only reducer state".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Bounded exact lexical material supplied only when a command interprets one
/// Scope location. It is transient and never persistence authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineScopeLocationWitness {
    scope_id: String,
    invocation_path: Vec<crate::InvocationPathSegment>,
    region_path: Vec<usize>,
}

impl MachineScopeLocationWitness {
    /// Assemble a resolver-owned exact Scope location witness.
    #[doc(hidden)]
    pub fn new(
        scope_id: String,
        invocation_path: Vec<crate::InvocationPathSegment>,
        region_path: Vec<usize>,
    ) -> Result<Self> {
        validate_identity("Machine Scope location", &scope_id)?;
        let witness = Self {
            scope_id,
            invocation_path,
            region_path,
        };
        account_read_bytes(
            "Machine Scope location witness",
            &witness.preimage(),
            &mut 0,
        )?;
        Ok(witness)
    }

    fn verify(&self, current: &MachineScopeCurrent) -> Result<()> {
        if self.scope_id != current.scope_id
            || canonical_digest(&self.invocation_path)? != current.invocation_path_digest
            || canonical_digest(&self.region_path)? != current.region_path_digest
        {
            return Err(CoreError::IdentityMismatch(format!(
                "Machine Scope {} location witness changed identity",
                self.scope_id
            )));
        }
        Ok(())
    }

    fn preimage(&self) -> (&str, &[crate::InvocationPathSegment], &[usize]) {
        (&self.scope_id, &self.invocation_path, &self.region_path)
    }
}

/// Validate a pinned executable frame using Core's structural Scope rules.
///
/// # Errors
///
/// Returns an error for a changed Plan, invocation path, lexical Scope,
/// immutable Scope witness, or out-of-range next step. This is structural
/// inspection; command admission separately authorizes execution in open scopes.
#[doc(hidden)]
pub fn validate_pinned_execution_frame(
    plan: &SealedPlan,
    location: &crate::ExecutionFrameLocation<'_>,
    scope: &MachineScopeCurrent,
    scope_invocation_path: &[crate::InvocationPathSegment],
    scope_region_path: &[usize],
) -> Result<()> {
    scope.verify()?;
    let witness = MachineScopeLocationWitness::new(
        scope.scope_id.clone(),
        scope_invocation_path.to_vec(),
        scope_region_path.to_vec(),
    )?;
    witness.verify(scope)?;
    let scope = crate::ScopeProjection {
        scope_id: scope.scope_id.clone(),
        parent_scope: scope.parent_scope.clone(),
        invocation_id: scope.invocation_id.clone(),
        invocation_path: scope_invocation_path.to_vec(),
        definition_id: scope.definition_id.clone(),
        region_path: scope_region_path.to_vec(),
        site_id: scope.site_id.clone(),
        status: scope.status,
        intents: BTreeSet::new(),
        intent_order: Vec::new(),
    };
    super::validate_plan_execution_frame(plan, location, &scope)
}

/// Bounded immutable Plan and Artifact material owned by one framework command.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct MachineMaterialAdmission {
    source_command_id: String,
    material_digest: String,
    plans: Vec<SealedPlan>,
    artifacts: Vec<ArtifactRecord>,
}

impl MachineMaterialAdmission {
    fn source_manifest(&self) -> MachineCommandBatchMaterialSource {
        MachineCommandBatchMaterialSource {
            source_command_id: self.source_command_id.clone(),
            plan_ids: self.plans.iter().map(|plan| plan.plan_id.clone()).collect(),
            artifacts: self
                .artifacts
                .iter()
                .map(|artifact| artifact.reference.clone())
                .collect(),
        }
    }
    /// Validate, canonicalize, and bind one framework-owned material set.
    ///
    /// # Errors
    ///
    /// Returns an error when the source identity or any material is invalid,
    /// repeated, oversized, or exceeds the closed item bounds.
    #[doc(hidden)]
    pub fn new(
        source_command_id: String,
        mut plans: Vec<SealedPlan>,
        mut artifacts: Vec<ArtifactRecord>,
    ) -> Result<Self> {
        validate_identity("Machine material source command", &source_command_id)?;
        if (plans.is_empty() && artifacts.is_empty())
            || plans.len() > MAX_MACHINE_MATERIAL_PLANS
            || artifacts.len() > MAX_MACHINE_MATERIAL_ARTIFACTS
        {
            return Err(CoreError::Validation(
                "Machine material admission is empty or exceeds its closed item bounds".to_owned(),
            ));
        }
        for plan in &plans {
            plan.verify()?;
        }
        let mut bytes = 0;
        for plan in &plans {
            account_read_bytes("Machine material Plan", plan, &mut bytes)?;
        }
        for artifact in &artifacts {
            artifact.validate()?;
            account_read_bytes("Machine material Artifact", artifact, &mut bytes)?;
        }
        plans.sort_by(|left, right| left.plan_id.cmp(&right.plan_id));
        artifacts
            .sort_by(|left, right| left.reference.artifact_id.cmp(&right.reference.artifact_id));
        if plans
            .windows(2)
            .any(|pair| pair[0].plan_id == pair[1].plan_id)
            || artifacts
                .windows(2)
                .any(|pair| pair[0].reference.artifact_id == pair[1].reference.artifact_id)
        {
            return Err(CoreError::Validation(
                "Machine material admission repeats an immutable identity".to_owned(),
            ));
        }
        let material_digest = content_id(
            MACHINE_MATERIAL_ADMISSION_DOMAIN,
            &(&source_command_id, &plans, &artifacts),
        )?;
        Ok(Self {
            source_command_id,
            material_digest,
            plans,
            artifacts,
        })
    }

    /// Owning framework command identity.
    pub fn source_command_id(&self) -> &str {
        &self.source_command_id
    }

    /// Content digest of the complete canonical material admission.
    pub fn material_digest(&self) -> &str {
        &self.material_digest
    }

    /// Canonically ordered proposed Plans.
    pub fn plans(&self) -> &[SealedPlan] {
        &self.plans
    }

    /// Canonically ordered proposed Artifacts.
    pub fn artifacts(&self) -> &[ArtifactRecord] {
        &self.artifacts
    }
}

/// Exact parent membership/absence reads for one material admission.
#[derive(Debug, Clone, PartialEq)]
pub struct MachineMaterialParentReads {
    plans: BTreeMap<String, Option<SealedPlan>>,
    artifacts: BTreeMap<String, Option<ArtifactRecord>>,
}

impl MachineMaterialParentReads {
    /// Assemble resolver-owned exact parent reads.
    #[doc(hidden)]
    pub fn new(
        plans: BTreeMap<String, Option<SealedPlan>>,
        artifacts: BTreeMap<String, Option<ArtifactRecord>>,
    ) -> Self {
        Self { plans, artifacts }
    }
}

/// Complete Core result for a framework-owned material-only admission.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedMachineMaterialAdmission {
    /// Owning framework command identity.
    pub source_command_id: String,
    /// Digest of the complete canonical proposed material.
    pub material_digest: String,
    /// Final Machine semantic frontier.
    pub frontier: MachineAuthorityFrontier,
    /// Exact Plan/Artifact persistent-root delta.
    pub delta: MachineRootDelta,
}

/// Closed immutable material proposed by one `StartRun` command.
///
/// The command binds every identity. The exact parent membership/absence reads
/// remain separate in [`MachineRunReadInputs`], so Core alone decides which
/// values are new admissions and which are exact immutable reuse.
#[derive(Debug, Clone, PartialEq)]
pub struct MachineStartRunMaterial {
    admission: MachineMaterialAdmission,
}

impl MachineStartRunMaterial {
    /// Assemble the complete closed immutable material proposed with StartRun.
    ///
    /// # Errors
    ///
    /// Returns an error when the source command or exact typed material is not
    /// a valid bounded Machine material admission.
    #[doc(hidden)]
    pub fn new(
        source_command_id: String,
        plan: SealedPlan,
        execution_binding: ArtifactRecord,
        input: ArtifactRecord,
    ) -> Result<Self> {
        if execution_binding.reference.kind != crate::EXECUTION_BINDING_ARTIFACT_KIND
            || input.reference.kind != crate::RUN_INPUT_ARTIFACT_KIND
        {
            return Err(CoreError::Validation(
                "StartRun material requires exact execution-binding and Run-input Artifact kinds"
                    .to_owned(),
            ));
        }
        Ok(Self {
            admission: MachineMaterialAdmission::new(
                source_command_id,
                vec![plan],
                vec![execution_binding, input],
            )?,
        })
    }

    /// Digest bound into the owning `StartRun` command.
    pub fn material_digest(&self) -> &str {
        self.admission.material_digest()
    }

    /// Generic bounded material admission embedded by this `StartRun`.
    pub fn admission(&self) -> &MachineMaterialAdmission {
        &self.admission
    }

    fn parts(&self) -> Result<(&SealedPlan, &ArtifactRecord, &ArtifactRecord)> {
        let [plan] = self.admission.plans.as_slice() else {
            return Err(CoreError::IdentityMismatch(
                "StartRun material does not contain exactly one Plan".to_owned(),
            ));
        };
        let mut execution_binding = None;
        let mut input = None;
        for artifact in &self.admission.artifacts {
            match artifact.reference.kind.as_str() {
                crate::EXECUTION_BINDING_ARTIFACT_KIND => execution_binding = Some(artifact),
                crate::RUN_INPUT_ARTIFACT_KIND => input = Some(artifact),
                _ => {
                    return Err(CoreError::IdentityMismatch(
                        "StartRun material contains an unrelated Artifact kind".to_owned(),
                    ));
                }
            }
        }
        match (execution_binding, input, self.admission.artifacts.len()) {
            (Some(execution_binding), Some(input), 2) => Ok((plan, execution_binding, input)),
            _ => Err(CoreError::IdentityMismatch(
                "StartRun material does not contain its exact execution binding and input"
                    .to_owned(),
            )),
        }
    }
}

/// Non-wire exact-key values assembled by the trusted durable resolver.
///
/// Map presence is significant: a present key with `None` is an authenticated
/// absence result, while a missing key means the command-shaped read set is
/// incomplete and must fail before semantic reduction starts.
#[derive(Debug, Clone, PartialEq)]
pub struct MachineRunReadInputs {
    /// Exact durable manifest revision under which every input was resolved.
    pub machine_revision: String,
    /// Target Run identity.
    pub run_id: String,
    /// Exact source root of the global Run-current map.
    pub runs_root: MachineMapRoot,
    /// Exact source root of the global fact map.
    pub facts_root: MachineMapRoot,
    /// Current Run leaf, or authenticated absence for `StartRun`.
    pub run: Option<MachineRunCurrent>,
    /// Canonical empty physical map token used to create a new Run's child
    /// collections and reducer indexes. Required only for `StartRun`.
    pub new_run_empty_root: Option<MachineMapRoot>,
    /// Canonical empty physical log token used to create a new Run's ordered
    /// collections and lineages. Required only for `StartRun`.
    pub new_run_empty_log: Option<MachineLogRoot>,
    /// Exact Plan key reads.
    pub plans: BTreeMap<String, Option<SealedPlan>>,
    /// Exact Artifact key reads.
    pub artifacts: BTreeMap<String, Option<ArtifactRecord>>,
    /// Exact Scope key reads.
    pub scopes: BTreeMap<String, Option<MachineScopeCurrent>>,
    /// Exact lexical witnesses for command-interpreted Scopes.
    pub scope_locations: BTreeMap<String, MachineScopeLocationWitness>,
    /// Exact Effect key reads.
    pub effects: BTreeMap<String, Option<crate::EffectProjection>>,
    /// Exact obligation key reads.
    pub obligations: BTreeMap<String, Option<ObligationProjection>>,
    /// Exact Attempt key reads.
    pub attempts: BTreeMap<String, Option<crate::AttemptProjection>>,
    /// Exact fact key reads.
    pub facts: BTreeMap<String, Option<String>>,
    /// Closed new-Run material, present only for `StartRun`.
    pub start_material: Option<MachineStartRunMaterial>,
    /// Bounded persistent-index pages needed by this command.
    pub index_pages: Vec<MachineRunIndexPage>,
    /// Bounded proposal-order pages needed by this command.
    pub log_pages: Vec<MachineRunLogPage>,
}

/// Closed, fully enumerated local view consumed by the pure reducer.
///
/// Both this type and [`MachineRunReadInputs`] are intentionally not
/// serializable. Construction validates every root, owner, frontier, key, and
/// command-shaped read before execution; a transport caller cannot author a
/// Store-resolution proof.
#[derive(Debug, Clone)]
pub struct MachineRunReadSet {
    inputs: MachineRunReadInputs,
    inline_scope: Option<InlineScopeClosure>,
}

#[derive(Default)]
struct ExactCommandReadKeys {
    plans: BTreeSet<String>,
    artifacts: BTreeSet<String>,
    scopes: BTreeSet<String>,
    locations: BTreeSet<String>,
    effects: BTreeSet<String>,
    obligations: BTreeSet<String>,
    attempts: BTreeSet<String>,
    facts: BTreeSet<String>,
}

impl MachineRunReadSet {
    /// Prepare one closed command-shaped read set. Missing keys are framework
    /// invariant failures, never a lazy-loading protocol.
    ///
    /// # Errors
    ///
    /// Returns an error when any input is not proved by the supplied frontier,
    /// exceeds a read bound, changes identity, or does not exactly match the
    /// command's required read shape.
    pub fn prepare(
        frontier: &MachineAuthorityFrontier,
        envelope: &CommandEnvelope,
        inputs: MachineRunReadInputs,
    ) -> Result<Self> {
        Self::prepare_with_inline(frontier, envelope, inputs, None)
    }

    fn prepare_with_inline(
        frontier: &MachineAuthorityFrontier,
        envelope: &CommandEnvelope,
        inputs: MachineRunReadInputs,
        inline: Option<MachineInlineScopeReadRequirement>,
    ) -> Result<Self> {
        verify_read_set_header(frontier, envelope, &inputs)?;
        verify_read_set_budget(&inputs, inline.as_ref())?;
        verify_read_set_run(envelope, &inputs)?;
        verify_plan_and_artifact_reads(envelope, &inputs)?;
        verify_run_child_reads(&inputs)?;
        verify_read_set_pages(&inputs)?;
        let inline_scope = inline
            .map(|requirement| InlineScopeClosure::verify(envelope, requirement, &inputs))
            .transpose()?;
        let reads = Self {
            inputs,
            inline_scope,
        };
        reads.require_command_reads(envelope)?;
        Ok(reads)
    }

    /// Borrow the pinned target Run current.
    pub fn run(&self) -> Option<&MachineRunCurrent> {
        self.inputs.run.as_ref()
    }

    fn require_plan(&self, plan_id: &str) -> Result<&SealedPlan> {
        self.inputs
            .plans
            .get(plan_id)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine Plan",
                key: plan_id.to_owned(),
            })?
            .as_ref()
            .ok_or_else(|| CoreError::NotFound(format!("plan {plan_id} does not exist")))
    }

    fn require_artifact(&self, reference: &ArtifactRef) -> Result<&ArtifactRecord> {
        let artifact = self
            .inputs
            .artifacts
            .get(&reference.artifact_id)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine Artifact",
                key: reference.artifact_id.clone(),
            })?
            .as_ref()
            .ok_or_else(|| {
                CoreError::NotFound(format!("Artifact {} does not exist", reference.artifact_id))
            })?;
        if artifact.reference != *reference {
            return Err(CoreError::IdentityMismatch(format!(
                "Artifact {} read changed its exact reference",
                reference.artifact_id
            )));
        }
        Ok(artifact)
    }

    fn require_artifact_id(&self, artifact_id: &str, kind: &str) -> Result<&ArtifactRecord> {
        let artifact = self
            .inputs
            .artifacts
            .get(artifact_id)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine Artifact",
                key: artifact_id.to_owned(),
            })?
            .as_ref()
            .ok_or_else(|| CoreError::NotFound(format!("Artifact {artifact_id} does not exist")))?;
        if artifact.reference.kind != kind {
            return Err(CoreError::Validation(format!(
                "Artifact {artifact_id} does not have exact kind {kind}"
            )));
        }
        Ok(artifact)
    }

    fn require_scope(&self, scope_id: &str) -> Result<&MachineScopeCurrent> {
        self.inputs
            .scopes
            .get(scope_id)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine Scope current",
                key: scope_id.to_owned(),
            })?
            .as_ref()
            .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))
    }

    fn require_scope_absent(&self, scope_id: &str) -> Result<()> {
        match self.inputs.scopes.get(scope_id) {
            Some(None) => Ok(()),
            Some(Some(_)) => Err(CoreError::IllegalTransition(format!(
                "scope {scope_id} already exists"
            ))),
            None => Err(CoreError::PinnedReadSetIncomplete {
                family: "Machine Scope current",
                key: scope_id.to_owned(),
            }),
        }
    }

    fn require_effect(&self, intent_id: &str) -> Result<&crate::EffectProjection> {
        self.inputs
            .effects
            .get(intent_id)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine Effect current",
                key: intent_id.to_owned(),
            })?
            .as_ref()
            .ok_or_else(|| CoreError::NotFound(format!("effect {intent_id} does not exist")))
    }

    fn require_effect_absent(&self, intent_id: &str) -> Result<()> {
        match self.inputs.effects.get(intent_id) {
            Some(None) => Ok(()),
            Some(Some(_)) => Err(CoreError::IllegalTransition(format!(
                "effect intent {intent_id} already exists"
            ))),
            None => Err(CoreError::PinnedReadSetIncomplete {
                family: "Machine Effect current",
                key: intent_id.to_owned(),
            }),
        }
    }

    fn require_attempt(&self, attempt_id: &str) -> Result<&crate::AttemptProjection> {
        self.inputs
            .attempts
            .get(attempt_id)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine Attempt current",
                key: attempt_id.to_owned(),
            })?
            .as_ref()
            .ok_or_else(|| CoreError::NotFound(format!("attempt {attempt_id} does not exist")))
    }

    fn require_attempt_absent(&self, attempt_id: &str) -> Result<()> {
        match self.inputs.attempts.get(attempt_id) {
            Some(None) => Ok(()),
            Some(Some(_)) => Err(CoreError::IllegalTransition(format!(
                "attempt {attempt_id} already exists"
            ))),
            None => Err(CoreError::PinnedReadSetIncomplete {
                family: "Machine Attempt current",
                key: attempt_id.to_owned(),
            }),
        }
    }

    fn require_location(&self, scope_id: &str) -> Result<&MachineScopeLocationWitness> {
        self.inputs.scope_locations.get(scope_id).ok_or_else(|| {
            CoreError::PinnedReadSetIncomplete {
                family: "Machine Scope location",
                key: scope_id.to_owned(),
            }
        })
    }

    fn require_command_reads(&self, envelope: &CommandEnvelope) -> Result<()> {
        let run = self.inputs.run.as_ref();
        if let Some(run) = run
            && !matches!(run.reducer_state, MachineRunReducerState::Ready)
        {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} is owned by a paged transition",
                run.run_id
            )));
        }
        match &envelope.command {
            Command::StartRun { .. } => {
                verify_start_run_material(envelope, &self.inputs)?;
            }
            Command::BeginAttempt { attempt_id, .. } => {
                self.require_attempt_absent(attempt_id)?;
            }
            Command::YieldAttempt { attempt_id, .. } => {
                self.require_attempt(attempt_id)?;
            }
            Command::AdvanceEpoch => {
                if let Some(attempt_id) = run.and_then(|run| run.active_attempt_id.as_deref()) {
                    self.require_attempt(attempt_id)?;
                }
            }
            Command::OpenScope {
                scope_id,
                parent_scope,
                invocation_path,
                ..
            } => {
                self.require_open_scope_reads(scope_id, parent_scope, invocation_path, run)?;
            }
            Command::ProposeEffect { .. } => {
                self.require_propose_effect_reads(&envelope.command, run)?;
            }
            Command::TransitionEffect { intent_id, .. } => {
                self.require_transition_effect_reads(intent_id)?;
            }
            Command::CommitScope { scope_id } | Command::AbortScope { scope_id } => {
                self.require_scope(scope_id)?;
                if self.inline_scope.is_some()
                    && let Some(parent) = &self.require_scope(scope_id)?.parent_scope
                {
                    self.require_scope(parent)?;
                }
            }
            Command::UpdateBinding { binding_context } => {
                self.require_artifact_id(binding_context, crate::EXECUTION_BINDING_ARTIFACT_KIND)?;
            }
            Command::MigrateRun {
                from_plan,
                to_plan,
                from_binding,
                to_binding,
                ..
            } => {
                self.require_plan(from_plan)?;
                self.require_plan(to_plan)?;
                self.require_artifact_id(from_binding, crate::EXECUTION_BINDING_ARTIFACT_KIND)?;
                self.require_artifact_id(to_binding, crate::EXECUTION_BINDING_ARTIFACT_KIND)?;
            }
            Command::RecordFact { key, .. } => {
                if !self.inputs.facts.contains_key(key) {
                    return Err(CoreError::PinnedReadSetIncomplete {
                        family: "Machine fact",
                        key: key.clone(),
                    });
                }
            }
            Command::CompleteRun { result } => {
                if let Some(result) = result {
                    self.require_artifact(result)?;
                }
            }
            Command::FailRun { failure } => {
                self.require_artifact(&failure.detail)?;
            }
            Command::CancelRun { reason } => {
                self.require_artifact(reason)?;
            }
        }
        self.verify_exact_command_shape(envelope)
    }

    fn require_open_scope_reads(
        &self,
        scope_id: &str,
        parent_scope: &str,
        invocation_path: &[crate::InvocationPathSegment],
        run: Option<&MachineRunCurrent>,
    ) -> Result<()> {
        let empty_map = self.inputs.new_run_empty_root.as_ref().ok_or_else(|| {
            CoreError::PinnedReadSetIncomplete {
                family: "Machine empty child root",
                key: scope_id.to_owned(),
            }
        })?;
        let empty_log = self.inputs.new_run_empty_log.as_ref().ok_or_else(|| {
            CoreError::PinnedReadSetIncomplete {
                family: "Machine empty child log",
                key: scope_id.to_owned(),
            }
        })?;
        if empty_map.entries != 0 || empty_log.len != 0 {
            return Err(CoreError::Validation(
                "new Machine Scope requires empty child roots".to_owned(),
            ));
        }
        self.require_scope_absent(scope_id)?;
        self.require_scope(parent_scope)?;
        self.require_location(parent_scope)?;
        for segment in invocation_path {
            self.require_scope(&segment.scope_id)?;
            self.require_location(&segment.scope_id)?;
        }
        let run = run.ok_or_else(|| {
            CoreError::NotFound("non-start command has no Run current".to_owned())
        })?;
        self.require_plan(&run.current_plan).map(|_| ())
    }

    fn require_propose_effect_reads(
        &self,
        command: &Command,
        run: Option<&MachineRunCurrent>,
    ) -> Result<()> {
        let Command::ProposeEffect {
            scope_id,
            invocation_id,
            invocation_path,
            site_id,
            occurrence,
            args,
            execution_binding,
            ..
        } = command
        else {
            return Err(CoreError::Validation(
                "Effect read validation requires a ProposeEffect command".to_owned(),
            ));
        };
        let run = run.ok_or_else(|| {
            CoreError::NotFound("non-start command has no Run current".to_owned())
        })?;
        self.require_scope(scope_id)?;
        self.require_location(scope_id)?;
        for segment in invocation_path {
            self.require_scope(&segment.scope_id)?;
            self.require_location(&segment.scope_id)?;
        }
        self.require_plan(&run.current_plan)?;
        self.require_artifact(args)?;
        self.require_artifact(execution_binding)?;
        let intent_id = effect_intent_id(&EffectIntentIdentityInput {
            run_id: &run.run_id,
            plan_id: &run.current_plan,
            invocation_id,
            site_id,
            scope_id,
            occurrence,
            args,
            effect_schema_version: crate::EFFECT_SCHEMA_VERSION,
        })?;
        self.require_effect_absent(&intent_id)
    }

    fn require_transition_effect_reads(&self, intent_id: &str) -> Result<()> {
        let effect = self.require_effect(intent_id)?;
        let scope = self.require_scope(&effect.scope_id)?;
        if effect.profile.mutation != crate::MutationKind::Mutating
            || scope.status != crate::ScopeStatus::ClosedCommitted
        {
            return Ok(());
        }
        let obligation_id = effect_obligation_id(intent_id)?;
        self.inputs
            .obligations
            .get(&obligation_id)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine obligation current",
                key: obligation_id.clone(),
            })?
            .as_ref()
            .ok_or_else(|| {
                CoreError::NotFound(format!("obligation {obligation_id} does not exist"))
            })
            .map(|_| ())
    }

    fn expected_command_read_keys(
        &self,
        envelope: &CommandEnvelope,
    ) -> Result<ExactCommandReadKeys> {
        let mut keys = ExactCommandReadKeys::default();
        let run = self.inputs.run.as_ref();
        match &envelope.command {
            Command::StartRun {
                plan_id,
                binding_context,
                input,
                ..
            } => {
                keys.plans.insert(plan_id.clone());
                keys.artifacts
                    .extend([binding_context.clone(), input.artifact_id.clone()]);
            }
            Command::BeginAttempt { attempt_id, .. } | Command::YieldAttempt { attempt_id, .. } => {
                keys.attempts.insert(attempt_id.clone());
            }
            Command::AdvanceEpoch => {
                if let Some(attempt_id) = run.and_then(|run| run.active_attempt_id.clone()) {
                    keys.attempts.insert(attempt_id);
                }
            }
            Command::OpenScope {
                scope_id,
                parent_scope,
                invocation_path,
                ..
            } => {
                let run = run.ok_or_else(|| {
                    CoreError::NotFound("non-start command has no Run current".to_owned())
                })?;
                keys.plans.insert(run.current_plan.clone());
                keys.scopes.extend([scope_id.clone(), parent_scope.clone()]);
                keys.locations.insert(parent_scope.clone());
                for segment in invocation_path {
                    keys.scopes.insert(segment.scope_id.clone());
                    keys.locations.insert(segment.scope_id.clone());
                }
            }
            Command::ProposeEffect { .. } => {
                Self::extend_propose_effect_read_keys(&mut keys, &envelope.command, run)?;
            }
            Command::TransitionEffect { intent_id, .. } => {
                keys.effects.insert(intent_id.clone());
                let effect = self.require_effect(intent_id)?;
                keys.scopes.insert(effect.scope_id.clone());
                if effect.profile.mutation == crate::MutationKind::Mutating
                    && self.require_scope(&effect.scope_id)?.status
                        == crate::ScopeStatus::ClosedCommitted
                {
                    keys.obligations.insert(effect_obligation_id(intent_id)?);
                }
            }
            Command::CommitScope { scope_id } | Command::AbortScope { scope_id } => {
                keys.scopes.insert(scope_id.clone());
                if let Some(closure) = &self.inline_scope {
                    keys.effects.extend(closure.effect_ids.iter().cloned());
                    keys.obligations
                        .extend(closure.obligation_ids.iter().cloned());
                    if let Some(parent) = &self.require_scope(scope_id)?.parent_scope {
                        keys.scopes.insert(parent.clone());
                    }
                }
            }
            Command::UpdateBinding { binding_context } => {
                keys.artifacts.insert(binding_context.clone());
            }
            Command::MigrateRun {
                from_plan,
                to_plan,
                from_binding,
                to_binding,
                ..
            } => {
                keys.plans.extend([from_plan.clone(), to_plan.clone()]);
                keys.artifacts
                    .extend([from_binding.clone(), to_binding.clone()]);
            }
            Command::RecordFact { key, .. } => {
                keys.facts.insert(key.clone());
            }
            Command::CompleteRun { result } => {
                if let Some(result) = result {
                    keys.artifacts.insert(result.artifact_id.clone());
                }
            }
            Command::FailRun { failure } => {
                keys.artifacts.insert(failure.detail.artifact_id.clone());
            }
            Command::CancelRun { reason } => {
                keys.artifacts.insert(reason.artifact_id.clone());
            }
        }
        Ok(keys)
    }

    fn extend_propose_effect_read_keys(
        keys: &mut ExactCommandReadKeys,
        command: &Command,
        run: Option<&MachineRunCurrent>,
    ) -> Result<()> {
        let Command::ProposeEffect {
            scope_id,
            invocation_id,
            invocation_path,
            site_id,
            occurrence,
            args,
            execution_binding,
            ..
        } = command
        else {
            return Err(CoreError::Validation(
                "Effect key derivation requires a ProposeEffect command".to_owned(),
            ));
        };
        let run = run.ok_or_else(|| {
            CoreError::NotFound("non-start command has no Run current".to_owned())
        })?;
        keys.plans.insert(run.current_plan.clone());
        keys.scopes.insert(scope_id.clone());
        keys.locations.insert(scope_id.clone());
        for segment in invocation_path {
            keys.scopes.insert(segment.scope_id.clone());
            keys.locations.insert(segment.scope_id.clone());
        }
        keys.artifacts.insert(args.artifact_id.clone());
        keys.artifacts.insert(execution_binding.artifact_id.clone());
        keys.effects
            .insert(effect_intent_id(&EffectIntentIdentityInput {
                run_id: &run.run_id,
                plan_id: &run.current_plan,
                invocation_id,
                site_id,
                scope_id,
                occurrence,
                args,
                effect_schema_version: crate::EFFECT_SCHEMA_VERSION,
            })?);
        Ok(())
    }

    fn verify_exact_command_shape(&self, envelope: &CommandEnvelope) -> Result<()> {
        let expected = self.expected_command_read_keys(envelope)?;
        let actual_plans = self.inputs.plans.keys().cloned().collect::<BTreeSet<_>>();
        let actual_artifacts = self
            .inputs
            .artifacts
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let actual_scopes = self.inputs.scopes.keys().cloned().collect::<BTreeSet<_>>();
        let actual_locations = self
            .inputs
            .scope_locations
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let actual_effects = self.inputs.effects.keys().cloned().collect::<BTreeSet<_>>();
        let actual_obligations = self
            .inputs
            .obligations
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let actual_attempts = self
            .inputs
            .attempts
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let actual_facts = self.inputs.facts.keys().cloned().collect::<BTreeSet<_>>();
        if actual_plans != expected.plans
            || actual_artifacts != expected.artifacts
            || actual_scopes != expected.scopes
            || actual_locations != expected.locations
            || actual_effects != expected.effects
            || actual_obligations != expected.obligations
            || actual_attempts != expected.attempts
            || actual_facts != expected.facts
            || self.inputs.index_pages.len() != usize::from(self.inline_scope.is_some())
            || self.inputs.log_pages.len() != usize::from(self.inline_scope.is_some())
        {
            return Err(CoreError::Validation(
                "Machine command-shaped read set contains missing or unrelated exact reads"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

fn verify_read_set_header(
    frontier: &MachineAuthorityFrontier,
    envelope: &CommandEnvelope,
    inputs: &MachineRunReadInputs,
) -> Result<()> {
    frontier.verify()?;
    validate_envelope(envelope)?;
    crate::validate_content_id("Machine pinned revision", &inputs.machine_revision)?;
    validate_identity("Machine read-set Run", &inputs.run_id)?;
    inputs.runs_root.verify()?;
    inputs.facts_root.verify()?;
    if inputs.run_id != envelope.run_id
        || inputs.runs_root != frontier.runs
        || inputs.facts_root != frontier.facts
    {
        return Err(CoreError::IdentityMismatch(
            "Machine read set does not match its Run or pinned global roots".to_owned(),
        ));
    }
    Ok(())
}

fn verify_read_set_budget(
    inputs: &MachineRunReadInputs,
    inline: Option<&MachineInlineScopeReadRequirement>,
) -> Result<()> {
    if inputs.index_pages.len() > MAX_PINNED_MACHINE_INDEX_PAGES {
        return Err(CoreError::Validation(format!(
            "Machine command-shaped read set has {} index pages; maximum is {MAX_PINNED_MACHINE_INDEX_PAGES}",
            inputs.index_pages.len()
        )));
    }
    let count = inputs
        .plans
        .len()
        .checked_add(inputs.artifacts.len())
        .and_then(|count| {
            count.checked_add(if inline.is_some() {
                0
            } else {
                inputs.scopes.len()
            })
        })
        .and_then(|count| count.checked_add(inputs.effects.len()))
        .and_then(|count| count.checked_add(inputs.obligations.len()))
        .and_then(|count| count.checked_add(inputs.attempts.len()))
        .and_then(|count| count.checked_add(inputs.facts.len()))
        .and_then(|count| count.checked_add(inputs.scope_locations.len()))
        .and_then(|count| count.checked_add(usize::from(inputs.start_material.is_some()) * 3))
        .and_then(|count| {
            inputs
                .index_pages
                .iter()
                .try_fold(count, |count, page| count.checked_add(page.entries.len()))
        })
        .and_then(|count| {
            inputs
                .log_pages
                .iter()
                .try_fold(count, |count, page| count.checked_add(page.entries.len()))
        })
        .ok_or_else(|| CoreError::Validation("Machine read-set size overflowed".to_owned()))?;
    let maximum = if let Some(requirement) = inline {
        verify_inline_structural_budget(inputs, requirement)?;
        MAX_INLINE_SCOPE_DYNAMIC_ENTRIES
    } else {
        MAX_PINNED_MACHINE_READ_SET_ENTRIES
    };
    if count > maximum {
        return Err(CoreError::Validation(format!(
            "Machine command-shaped read set has {count} entries; maximum is {MAX_PINNED_MACHINE_READ_SET_ENTRIES}"
        )));
    }
    verify_read_set_byte_budget(inputs)
}

fn verify_inline_structural_budget(
    inputs: &MachineRunReadInputs,
    requirement: &MachineInlineScopeReadRequirement,
) -> Result<()> {
    let target = inputs
        .scopes
        .get(&requirement.scope_id)
        .and_then(Option::as_ref)
        .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
            family: "inline Scope",
            key: requirement.scope_id.clone(),
        })?;
    let mut expected = BTreeSet::from([requirement.scope_id.as_str()]);
    expected.extend(target.parent_scope.as_deref());
    if expected.len() > MAX_INLINE_SCOPE_STRUCTURAL_ENTRIES
        || inputs.scopes.keys().map(String::as_str).ne(expected)
        || inputs.scopes.values().any(Option::is_none)
    {
        return Err(CoreError::Validation(
            "inline Scope structure must contain only the exact target and direct parent"
                .to_owned(),
        ));
    }
    Ok(())
}

fn verify_read_set_run(envelope: &CommandEnvelope, inputs: &MachineRunReadInputs) -> Result<()> {
    match (&inputs.run, &envelope.command) {
        (None, Command::StartRun { .. }) => {
            let empty = inputs.new_run_empty_root.as_ref().ok_or_else(|| {
                CoreError::PinnedReadSetIncomplete {
                    family: "Machine empty child root",
                    key: inputs.run_id.clone(),
                }
            })?;
            empty.verify()?;
            let empty_log = inputs.new_run_empty_log.as_ref().ok_or_else(|| {
                CoreError::PinnedReadSetIncomplete {
                    family: "Machine empty child log",
                    key: inputs.run_id.clone(),
                }
            })?;
            empty_log.verify()?;
            if empty.entries != 0 || empty_log.len != 0 {
                return Err(CoreError::Validation(
                    "new Machine Run requires empty child map and log roots".to_owned(),
                ));
            }
        }
        (Some(_), Command::StartRun { .. }) => {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} already exists",
                inputs.run_id
            )));
        }
        (Some(run), _) => {
            run.verify()?;
            if run.run_id != inputs.run_id {
                return Err(CoreError::IdentityMismatch(
                    "Machine read set changed the current Run".to_owned(),
                ));
            }
        }
        (None, _) => {
            return Err(CoreError::NotFound(format!(
                "Run {} does not exist",
                inputs.run_id
            )));
        }
    }
    if inputs.run.is_some()
        && !matches!(envelope.command, Command::OpenScope { .. })
        && (inputs.new_run_empty_root.is_some() || inputs.new_run_empty_log.is_some())
    {
        return Err(CoreError::Validation(
            "existing Machine Run read set carried a genesis child root".to_owned(),
        ));
    }
    Ok(())
}

fn verify_artifact_record(record: &ArtifactRecord) -> Result<()> {
    record.validate()
}

fn verify_start_run_material(
    envelope: &CommandEnvelope,
    inputs: &MachineRunReadInputs,
) -> Result<()> {
    let Command::StartRun {
        plan_id,
        binding_context,
        input,
        material_digest,
        initial_attempt,
    } = &envelope.command
    else {
        if inputs.start_material.is_some() {
            return Err(CoreError::Validation(
                "non-StartRun read set carried StartRun material".to_owned(),
            ));
        }
        return Ok(());
    };
    let material =
        inputs
            .start_material
            .as_ref()
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine StartRun material",
                key: envelope.run_id.clone(),
            })?;
    let (plan, execution_binding, input_record) = material.parts()?;
    plan.verify()?;
    verify_artifact_record(execution_binding)?;
    verify_artifact_record(input_record)?;
    initial_attempt.verify(binding_context)?;
    if material.admission.source_command_id() != envelope.command_id
        || material.material_digest() != material_digest
        || plan.plan_id != *plan_id
        || execution_binding.reference.artifact_id != *binding_context
        || input_record.reference != *input
    {
        return Err(CoreError::IdentityMismatch(
            "StartRun command does not bind its exact Plan, execution binding, and input material"
                .to_owned(),
        ));
    }
    let retained_plan =
        inputs
            .plans
            .get(plan_id)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine StartRun Plan parent",
                key: plan_id.clone(),
            })?;
    if retained_plan
        .as_ref()
        .is_some_and(|retained| retained != plan)
    {
        return Err(CoreError::IdentityMismatch(
            "StartRun Plan parent has conflicting immutable content".to_owned(),
        ));
    }
    for record in [execution_binding, input_record] {
        let id = &record.reference.artifact_id;
        let retained =
            inputs
                .artifacts
                .get(id)
                .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                    family: "Machine StartRun Artifact parent",
                    key: id.clone(),
                })?;
        if retained.as_ref().is_some_and(|value| value != record) {
            return Err(CoreError::IdentityMismatch(format!(
                "StartRun Artifact {id} has conflicting immutable content"
            )));
        }
    }
    let value: serde_json::Value = crate::decode_json(&input_record.bytes)?;
    if crate::canonical_bytes(&value)? != input_record.bytes {
        return Err(CoreError::Validation(
            "StartRun input Artifact is not strict canonical JSON".to_owned(),
        ));
    }
    let entry = plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == plan.candidate.entry)
        .ok_or_else(|| CoreError::NotFound("StartRun Plan entry is missing".to_owned()))?;
    crate::ir::validate_schema_instance("Run input", &entry.input_schema, &value)
}

fn verify_plan_and_artifact_reads(
    envelope: &CommandEnvelope,
    inputs: &MachineRunReadInputs,
) -> Result<()> {
    for (key, value) in &inputs.plans {
        crate::validate_content_id("Machine Plan read key", key)?;
        if let Some(plan) = value {
            plan.verify()?;
        }
        if value.as_ref().is_some_and(|plan| &plan.plan_id != key) {
            return Err(CoreError::IdentityMismatch(format!(
                "Machine Plan read key {key} changed identity"
            )));
        }
    }
    for (key, value) in &inputs.artifacts {
        crate::validate_content_id("Machine Artifact read key", key)?;
        if let Some(artifact) = value {
            verify_artifact_record(artifact)?;
        }
        if value
            .as_ref()
            .is_some_and(|artifact| &artifact.reference.artifact_id != key)
        {
            return Err(CoreError::IdentityMismatch(format!(
                "Machine Artifact read key {key} changed identity"
            )));
        }
    }
    verify_start_run_material(envelope, inputs)
}

fn verify_run_child_reads(inputs: &MachineRunReadInputs) -> Result<()> {
    for (key, value) in &inputs.scopes {
        validate_identity("Machine Scope read key", key)?;
        if let Some(scope) = value {
            scope.verify()?;
            if scope.scope_id != *key {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine Scope read key {key} changed identity"
                )));
            }
        }
    }
    for (key, witness) in &inputs.scope_locations {
        validate_identity("Machine Scope location key", key)?;
        let scope = inputs
            .scopes
            .get(key)
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine Scope current",
                key: key.clone(),
            })?
            .as_ref()
            .ok_or_else(|| CoreError::NotFound(format!("scope {key} does not exist")))?;
        witness.verify(scope)?;
    }
    verify_effect_and_obligation_reads(inputs)?;
    for (key, value) in &inputs.attempts {
        crate::validate_content_id("Machine Attempt read key", key)?;
        if let Some(attempt) = value {
            verify_attempt_read(attempt)?;
        }
        if value
            .as_ref()
            .is_some_and(|attempt| attempt.attempt_id != *key)
        {
            return Err(CoreError::IdentityMismatch(format!(
                "Machine Attempt read key {key} changed identity"
            )));
        }
    }
    for (key, value) in &inputs.facts {
        validate_identity("Machine fact read key", key)?;
        if let Some(value) = value {
            crate::validate_content_id("Machine fact value", value)?;
        }
    }
    Ok(())
}

fn verify_effect_and_obligation_reads(inputs: &MachineRunReadInputs) -> Result<()> {
    for (key, value) in &inputs.effects {
        crate::validate_content_id("Machine Effect read key", key)?;
        if let Some(effect) = value {
            verify_effect_read(effect)?;
        }
        if value
            .as_ref()
            .is_some_and(|effect| effect.intent_id != *key)
        {
            return Err(CoreError::IdentityMismatch(format!(
                "Machine Effect read key {key} changed identity"
            )));
        }
    }
    for (key, value) in &inputs.obligations {
        crate::validate_content_id("Machine obligation read key", key)?;
        if let Some(obligation) = value {
            verify_obligation_read(obligation)?;
        }
        if value
            .as_ref()
            .is_some_and(|obligation| obligation.obligation_id != *key)
        {
            return Err(CoreError::IdentityMismatch(format!(
                "Machine obligation read key {key} changed identity"
            )));
        }
    }
    Ok(())
}

fn verify_read_set_pages(inputs: &MachineRunReadInputs) -> Result<()> {
    let mut page_selectors = BTreeSet::new();
    for page in &inputs.index_pages {
        page.verify_local()?;
        if !page_selectors.insert(page.selector.clone())
            || page.cursor().is_some()
            || page.next_cursor().is_some()
            || u64::try_from(page.entries.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?
                != page.source().entries
        {
            return Err(CoreError::Validation(
                "ordinary Machine reduction requires one complete terminal page per selector"
                    .to_owned(),
            ));
        }
        if page.run_id != inputs.run_id {
            return Err(CoreError::IdentityMismatch(
                "Machine index page belongs to another Run".to_owned(),
            ));
        }
        verify_index_page_source(inputs.run.as_ref(), &inputs.scopes, page)?;
    }
    let mut log_selectors = BTreeSet::new();
    for page in &inputs.log_pages {
        page.verify_local()?;
        if page.run_id != inputs.run_id
            || !log_selectors.insert(page.selector.clone())
            || page.start() != 0
            || !page.is_terminal()?
        {
            return Err(CoreError::IdentityMismatch(
                "ordinary Machine reduction requires one complete Run-log page per selector"
                    .to_owned(),
            ));
        }
        verify_log_page_source(inputs.run.as_ref(), &inputs.scopes, page)?;
    }
    Ok(())
}

fn verify_read_set_byte_budget(inputs: &MachineRunReadInputs) -> Result<()> {
    let mut total = 0_usize;
    let start_authority = inputs.start_material.as_ref().map(|material| {
        (
            material.admission().source_command_id(),
            material.material_digest(),
        )
    });
    account_read_bytes(
        "Machine read-set fixed authority",
        &(
            &inputs.run_id,
            &inputs.machine_revision,
            &inputs.runs_root,
            &inputs.facts_root,
            &inputs.run,
            &inputs.new_run_empty_root,
            &inputs.new_run_empty_log,
            start_authority,
        ),
        &mut total,
    )?;
    if let Some(material) = &inputs.start_material {
        account_material_leaves(material.admission(), &mut total)?;
    }
    for (key, value) in &inputs.plans {
        account_material_parent_read("Machine Plan read", key, value.as_ref(), &mut total)?;
    }
    for (key, value) in &inputs.artifacts {
        account_material_parent_read("Machine Artifact read", key, value.as_ref(), &mut total)?;
    }
    for value in &inputs.scopes {
        account_read_bytes("Machine Scope read", &value, &mut total)?;
    }
    for value in &inputs.scope_locations {
        account_read_bytes(
            "Machine Scope location read",
            &value.1.preimage(),
            &mut total,
        )?;
    }
    for value in &inputs.effects {
        account_read_bytes("Machine Effect read", &value, &mut total)?;
    }
    for value in &inputs.obligations {
        account_read_bytes("Machine obligation read", &value, &mut total)?;
    }
    for value in &inputs.attempts {
        account_read_bytes("Machine Attempt read", &value, &mut total)?;
    }
    for value in &inputs.facts {
        account_read_bytes("Machine fact read", &value, &mut total)?;
    }
    for page in &inputs.index_pages {
        account_read_bytes("Machine Run-index page", &page.budget(), &mut total)?;
    }
    for page in &inputs.log_pages {
        account_read_bytes("Machine Run-log page", &page.budget(), &mut total)?;
    }
    Ok(())
}

fn account_read_bytes<T: serde::Serialize>(kind: &str, value: &T, total: &mut usize) -> Result<()> {
    let bytes = crate::canonical_bytes(value)?;
    if bytes.len() > MAX_PINNED_MACHINE_READ_LEAF_BYTES {
        return Err(CoreError::Validation(format!(
            "{kind} has {} canonical bytes; maximum is {MAX_PINNED_MACHINE_READ_LEAF_BYTES}",
            bytes.len()
        )));
    }
    *total = total
        .checked_add(bytes.len())
        .ok_or_else(|| CoreError::Validation("Machine read-set byte size overflowed".to_owned()))?;
    if *total > MAX_PINNED_MACHINE_READ_SET_BYTES {
        return Err(CoreError::Validation(format!(
            "Machine command-shaped read set has {total} canonical bytes; maximum is {MAX_PINNED_MACHINE_READ_SET_BYTES}"
        )));
    }
    Ok(())
}

fn verify_index_page_source(
    run: Option<&MachineRunCurrent>,
    scopes: &BTreeMap<String, Option<MachineScopeCurrent>>,
    page: &MachineRunIndexPage,
) -> Result<()> {
    let run = run.ok_or_else(|| CoreError::PinnedReadSetIncomplete {
        family: "Machine Run current",
        key: page.run_id.clone(),
    })?;
    let expected = match &page.selector {
        MachineRunIndexSelector::GovernanceEffects => &run.indexes.governance_effects,
        MachineRunIndexSelector::UnknownEffects => &run.indexes.unknown_effects,
        MachineRunIndexSelector::PendingEffects => &run.indexes.pending_effects,
        MachineRunIndexSelector::TerminalTransitionEffects => {
            &run.indexes.terminal_transition_effects
        }
        MachineRunIndexSelector::OpenScopes => &run.indexes.open_scopes,
        MachineRunIndexSelector::UnresolvedObligations => &run.indexes.unresolved_obligations,
        MachineRunIndexSelector::ScopeEffects { scope_id }
        | MachineRunIndexSelector::ScopeMutatingEffects { scope_id }
        | MachineRunIndexSelector::ScopeAbortTransitions { scope_id }
        | MachineRunIndexSelector::ScopeAbortBlockers { scope_id } => {
            let scope = scopes
                .get(scope_id)
                .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                    family: "Machine Scope current",
                    key: scope_id.clone(),
                })?
                .as_ref()
                .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))?;
            match &page.selector {
                MachineRunIndexSelector::ScopeEffects { .. } => &scope.effects,
                MachineRunIndexSelector::ScopeMutatingEffects { .. } => &scope.mutating_effects,
                MachineRunIndexSelector::ScopeAbortTransitions { .. } => &scope.abort_transitions,
                MachineRunIndexSelector::ScopeAbortBlockers { .. } => &scope.abort_blockers,
                _ => unreachable!("scope selector was matched above"),
            }
        }
    };
    if page.source() != expected {
        return Err(CoreError::IdentityMismatch(
            "Machine index page source does not match the pinned current root".to_owned(),
        ));
    }
    Ok(())
}

fn verify_log_page_source(
    run: Option<&MachineRunCurrent>,
    scopes: &BTreeMap<String, Option<MachineScopeCurrent>>,
    page: &MachineRunLogPage,
) -> Result<()> {
    let run = run.ok_or_else(|| CoreError::PinnedReadSetIncomplete {
        family: "Machine Run current",
        key: page.run_id.clone(),
    })?;
    let expected = match &page.selector {
        MachineRunLogSelector::Scopes => &run.order.scopes,
        MachineRunLogSelector::Effects => &run.order.effects,
        MachineRunLogSelector::Obligations => &run.order.obligations,
        MachineRunLogSelector::Attempts => &run.order.attempts,
        MachineRunLogSelector::Plans => &run.order.plans,
        MachineRunLogSelector::Bindings => &run.order.bindings,
        MachineRunLogSelector::ScopeEffects { scope_id }
        | MachineRunLogSelector::ScopeMutatingEffects { scope_id } => {
            let scope = scopes
                .get(scope_id)
                .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                    family: "Machine Scope current",
                    key: scope_id.clone(),
                })?
                .as_ref()
                .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))?;
            match &page.selector {
                MachineRunLogSelector::ScopeEffects { .. } => &scope.effect_order,
                MachineRunLogSelector::ScopeMutatingEffects { .. } => &scope.mutating_effect_order,
                _ => unreachable!("scope log selector was matched above"),
            }
        }
    };
    if page.source() != expected {
        return Err(CoreError::IdentityMismatch(
            "Machine Run-log page source does not match the pinned current root".to_owned(),
        ));
    }
    Ok(())
}

fn verify_effect_read(effect: &crate::EffectProjection) -> Result<()> {
    crate::validate_content_id("Machine Effect", &effect.intent_id)?;
    crate::validate_content_id("Machine Effect origin Plan", &effect.origin_plan_id)?;
    validate_identity("Machine Effect Scope", &effect.scope_id)?;
    crate::validate_content_id("Machine Effect invocation", &effect.invocation_id)?;
    crate::validate_semantic_id("Machine Effect definition", &effect.definition_id)?;
    crate::validate_semantic_id("Machine Effect site", &effect.site_id)?;
    validate_identity("Machine Effect occurrence", &effect.occurrence)?;
    if effect.effect_schema_version != crate::EFFECT_SCHEMA_VERSION {
        return Err(CoreError::Validation(format!(
            "Machine Effect {} has unsupported schema version {:?}",
            effect.intent_id, effect.effect_schema_version
        )));
    }
    effect.args.validate()?;
    effect.execution_binding.validate()?;
    crate::validate_content_id(
        "Machine Effect occurrence binding",
        &effect.occurrence_binding,
    )?;
    crate::model::verify_effect_reducer_state(effect)
}

fn verify_obligation_read(obligation: &ObligationProjection) -> Result<()> {
    crate::validate_content_id("Machine obligation", &obligation.obligation_id)?;
    crate::validate_content_id("Machine obligation Effect", &obligation.intent_id)?;
    if obligation.obligation_id != effect_obligation_id(&obligation.intent_id)?
        || !obligation.blocking
    {
        return Err(CoreError::IdentityMismatch(format!(
            "Machine obligation {} is not reducer-derived",
            obligation.obligation_id
        )));
    }
    Ok(())
}

fn verify_attempt_read(attempt: &crate::AttemptProjection) -> Result<()> {
    crate::validate_content_id("Machine Attempt", &attempt.attempt_id)?;
    crate::validate_content_id("Machine Attempt continuation", &attempt.continuation_id)?;
    crate::validate_content_id(
        "Machine Attempt occurrence binding",
        &attempt.occurrence_binding,
    )?;
    if attempt.continuation_epoch > crate::MAX_EXACT_INTEGER
        || attempt.execution_fence == 0
        || attempt.execution_fence > crate::MAX_EXACT_INTEGER
    {
        return Err(CoreError::Validation(
            "Machine Attempt epoch or execution fence exceeds the closed range".to_owned(),
        ));
    }
    Ok(())
}

/// Closed physical root target returned by the durable map applier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub enum MachineRunRootUpdateTarget {
    /// Global Run-current map.
    Runs,
    /// Global fact map.
    Facts,
    /// Global in-progress command reservation map.
    PendingCommands,
    /// Global persisted paged-transition map.
    PagedTransitions,
    /// Proposal-only Plan map rooted by a pending paged transition.
    PagedMaterialPlans,
    /// Proposal-only Artifact map rooted by a pending paged transition.
    PagedMaterialArtifacts,
    /// Run Scope-current map.
    Scopes,
    /// Run Effect-current map.
    Effects,
    /// Run obligation-current map.
    Obligations,
    /// Run Attempt-current map.
    Attempts,
    /// One global or per-scope reducer membership index.
    Index(MachineRunIndexSelector),
    /// One proposal-order Run or Scope log.
    Log(MachineRunLogSelector),
}

/// Closed physical root result kind.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum MachinePhysicalRoot {
    /// Persistent keyed map.
    Map(MachineMapRoot),
    /// Persistent proposal-order log.
    Log(MachineLogRoot),
}

impl MachinePhysicalRoot {
    fn verify(&self) -> Result<()> {
        match self {
            Self::Map(root) => root.verify()?,
            Self::Log(root) => root.verify()?,
        }
        Ok(())
    }

    fn count(&self) -> u64 {
        match self {
            Self::Map(root) => root.entries,
            Self::Log(root) => root.len,
        }
    }
}

/// One Store-computed result root supplied only to the consuming finish seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineRunRootUpdate {
    target: MachineRunRootUpdateTarget,
    parent: MachinePhysicalRoot,
    mutation_digest: String,
    result: MachinePhysicalRoot,
}

/// One exact physical apply requested by a prepared reducer stage.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct MachinePreparedRootMutation {
    target: MachineRunRootUpdateTarget,
    parent: MachinePhysicalRoot,
    expected_count: u64,
    typed: MachineTypedRootMutation,
    mutation_digest: String,
}

/// Complete typed Store apply bound by one prepared physical-root mutation.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "mutation", content = "apply", rename_all = "snake_case")]
pub enum MachineTypedRootMutation {
    /// Insert complete immutable proposed Plans into a private staging map.
    PutMaterialPlans(BTreeMap<String, SealedPlan>),
    /// Insert complete immutable proposed Artifacts into a private staging map.
    PutMaterialArtifacts(BTreeMap<String, ArtifactRecord>),
    /// Insert or replace Run-current leaves.
    PutRuns(BTreeMap<String, MachineRunCurrent>),
    /// Insert or replace Scope-current leaves.
    PutScopes(BTreeMap<String, MachineScopeCurrent>),
    /// Insert or replace Effect-current leaves.
    PutEffects(BTreeMap<String, crate::EffectProjection>),
    /// Insert or replace obligation-current leaves.
    PutObligations(BTreeMap<String, ObligationProjection>),
    /// Insert or replace Attempt-current leaves.
    PutAttempts(BTreeMap<String, crate::AttemptProjection>),
    /// Insert immutable global fact leaves.
    PutFacts(BTreeMap<String, String>),
    /// Apply exact reducer-index membership changes in order.
    UpdateMembership(Vec<MachineRunIndexMembershipDelta>),
    /// Append exact values to one proposal-order log.
    AppendLog(Vec<MachineRunLogAppendDelta>),
    /// Insert a command-to-transition reservation.
    ReserveCommand {
        /// Globally unique command identity.
        command_id: String,
        /// Exact owning transition identity.
        transition_id: String,
    },
    /// Insert or replace one persisted transition leaf.
    PutPagedTransition(Box<MachinePagedTransitionCurrent>),
    /// Remove the exact command reservation at final admission.
    RemoveCommandReservation {
        /// Globally unique command identity.
        command_id: String,
        /// Expected owning transition identity.
        transition_id: String,
    },
    /// Remove the exact persisted transition at final admission.
    RemovePagedTransition {
        /// Exact transition identity and key.
        transition_id: String,
        /// Digest of the exact final transition leaf.
        transition_digest: String,
    },
}

impl MachinePreparedRootMutation {
    /// Closed physical family to update.
    pub const fn target(&self) -> &MachineRunRootUpdateTarget {
        &self.target
    }

    /// Exact physical parent root pinned by the semantic preparation.
    pub const fn parent(&self) -> &MachinePhysicalRoot {
        &self.parent
    }

    /// Expected result cardinality after the typed mutation.
    pub const fn expected_count(&self) -> u64 {
        self.expected_count
    }

    /// Complete typed operation the Store must apply to `parent`.
    pub const fn typed(&self) -> &MachineTypedRootMutation {
        &self.typed
    }

    /// Digest binding the target, parent, and complete typed mutations.
    pub fn mutation_digest(&self) -> &str {
        &self.mutation_digest
    }

    /// Bind a Store-computed result to this exact prepared apply.
    #[doc(hidden)]
    pub fn bind_result(&self, result: MachinePhysicalRoot) -> MachineRunRootUpdate {
        MachineRunRootUpdate {
            target: self.target.clone(),
            parent: self.parent.clone(),
            mutation_digest: self.mutation_digest.clone(),
            result,
        }
    }
}

/// Exact command/idempotency authority resolved under the pinned Store roots.
#[derive(Debug, Clone, PartialEq)]
pub enum MachinePinnedCommandProof {
    /// The command is absent from both hot and archived authority.
    Vacant {
        /// Exact archived-command non-membership proof.
        index_proof: MachineCommandIndexProof,
    },
    /// The command remains in the hot keyed current maps.
    Retained {
        /// Exact retained private command record.
        record: ArchivedCommandRecord,
        /// Exact admission bound to that record.
        admission: CommandAdmission,
        /// Original archived-index non-membership authority.
        index_proof: MachineCommandIndexProof,
    },
    /// The command moved into the immutable command archive.
    Archived {
        /// Exact current-root membership proof.
        index_proof: MachineCommandIndexProof,
        /// Exact immutable archived entry.
        entry: MachineCommandArchiveEntry,
    },
    /// The command is globally reserved by an in-progress paged transition.
    Pending {
        /// Exact pending leaf resolved from the global command-reservation map.
        transition: Box<MachinePagedTransitionCurrent>,
    },
}

enum MachinePinnedCommandLookup {
    Vacant,
    Replay(CommandReceipt),
    Pending(Box<MachinePagedTransitionCurrent>),
}

impl MachinePinnedCommandProof {
    /// Assemble a trusted resolver result proving that the command is absent.
    #[doc(hidden)]
    pub fn vacant(index_proof: MachineCommandIndexProof) -> Self {
        Self::Vacant { index_proof }
    }

    /// Assemble a trusted resolver result for a retained command.
    #[doc(hidden)]
    pub fn retained(
        record: ArchivedCommandRecord,
        admission: CommandAdmission,
        index_proof: MachineCommandIndexProof,
    ) -> Self {
        Self::Retained {
            record,
            admission,
            index_proof,
        }
    }

    /// Assemble a cryptographically verified archived command lookup.
    #[doc(hidden)]
    pub fn archived(
        index_proof: MachineCommandIndexProof,
        entry: MachineCommandArchiveEntry,
    ) -> Self {
        Self::Archived { index_proof, entry }
    }

    /// Assemble one exact global pending-command reservation lookup.
    #[doc(hidden)]
    pub fn pending(transition: MachinePagedTransitionCurrent) -> Self {
        Self::Pending {
            transition: Box::new(transition),
        }
    }

    fn verify(
        &self,
        frontier: &MachineAuthorityFrontier,
        envelope: &CommandEnvelope,
    ) -> Result<MachinePinnedCommandLookup> {
        let semantic_hash = canonical_digest(envelope)?;
        match self {
            Self::Vacant { index_proof } => {
                if index_proof.command_id != envelope.command_id || index_proof.value.is_some() {
                    return Err(CoreError::IdentityMismatch(
                        "pinned new command has the wrong archive non-membership proof".to_owned(),
                    ));
                }
                index_proof.verify(&frontier.command_index_root)?;
                Ok(MachinePinnedCommandLookup::Vacant)
            }
            Self::Retained {
                record,
                admission,
                index_proof,
            } => {
                record.verify()?;
                verify_admission_shape(admission)?;
                if index_proof.command_id != envelope.command_id || index_proof.value.is_some() {
                    return Err(CoreError::IdentityMismatch(
                        "retained pinned command has the wrong archive non-membership proof"
                            .to_owned(),
                    ));
                }
                index_proof.verify(&frontier.command_index_root)?;
                let private = record.to_private();
                verify_admission_record(admission, &private)?;
                if record.envelope != *envelope || record.semantic_hash != semantic_hash {
                    return Err(CoreError::CommandReuse(format!(
                        "command ID {} was already used with different semantics",
                        envelope.command_id
                    )));
                }
                Ok(MachinePinnedCommandLookup::Replay(record.receipt.clone()))
            }
            Self::Archived { index_proof, entry } => {
                if index_proof.command_id != envelope.command_id {
                    return Err(CoreError::IdentityMismatch(
                        "archived pinned command proof names another command".to_owned(),
                    ));
                }
                index_proof.verify(&frontier.command_index_root)?;
                entry.verify()?;
                let value = index_proof.value.as_ref().ok_or_else(|| {
                    CoreError::Validation(
                        "archived pinned command lookup carried non-membership".to_owned(),
                    )
                })?;
                if entry.admission.command_id != envelope.command_id
                    || value.admission_id != entry.admission.admission_id
                    || value.archive_entry_digest != entry.identity()?
                {
                    return Err(CoreError::IdentityMismatch(
                        "archived pinned command proof does not bind its complete entry".to_owned(),
                    ));
                }
                if entry.command.envelope != *envelope
                    || entry.command.semantic_hash != semantic_hash
                {
                    return Err(CoreError::CommandReuse(format!(
                        "command ID {} was already used with different semantics",
                        envelope.command_id
                    )));
                }
                Ok(MachinePinnedCommandLookup::Replay(
                    entry.command.receipt.clone(),
                ))
            }
            Self::Pending { transition } => {
                transition.verify()?;
                if transition.envelope != *envelope || transition.command_hash != semantic_hash {
                    return Err(CoreError::CommandReuse(format!(
                        "command ID {} was already reserved with different semantics",
                        envelope.command_id
                    )));
                }
                Ok(MachinePinnedCommandLookup::Pending(transition.clone()))
            }
        }
    }

    fn vacant_proof(&self) -> Option<&MachineCommandIndexProof> {
        match self {
            Self::Vacant { index_proof } => Some(index_proof),
            Self::Retained { .. } | Self::Archived { .. } | Self::Pending { .. } => None,
        }
    }
}

fn verify_admission_shape(admission: &CommandAdmission) -> Result<()> {
    if admission.admission_version != COMMAND_ADMISSION_VERSION
        || admission.sequence == 0
        || admission.sequence > crate::MAX_EXACT_INTEGER
        || admission.command_id.is_empty()
        || admission.admission_id != admission.expected_id()?
    {
        return Err(CoreError::IdentityMismatch(format!(
            "command admission {} has malformed standalone authority",
            admission.admission_id
        )));
    }
    Ok(())
}

fn pinned_admission_parent(
    frontier: &MachineAuthorityFrontier,
) -> Result<Option<CommandAdmissionParent<'_>>> {
    match (
        frontier.admission_sequence,
        frontier.admission_head.as_deref(),
    ) {
        (0, None) => Ok(None),
        (sequence, Some(admission_id)) if sequence > 0 => Ok(Some(CommandAdmissionParent {
            sequence,
            admission_id,
        })),
        _ => Err(CoreError::IdentityMismatch(
            "pinned Machine admission frontier is incomplete".to_owned(),
        )),
    }
}

fn bounded_authority_machine(reads: &MachineRunReadSet) -> Result<Machine> {
    let mut machine = Machine::new();
    machine.plans = reads
        .inputs
        .plans
        .iter()
        .filter_map(|(id, plan)| plan.clone().map(|plan| (id.clone(), plan)))
        .collect();
    machine.artifacts = reads
        .inputs
        .artifacts
        .iter()
        .filter_map(|(id, artifact)| artifact.clone().map(|artifact| (id.clone(), artifact)))
        .collect();
    if let Some(material) = &reads.inputs.start_material {
        let (plan, execution_binding, input) = material.parts()?;
        machine.plans.insert(plan.plan_id.clone(), plan.clone());
        for artifact in [execution_binding, input] {
            machine
                .artifacts
                .insert(artifact.reference.artifact_id.clone(), artifact.clone());
        }
    }
    if let Some(current) = &reads.inputs.run {
        machine.projection.runs.insert(
            current.run_id.clone(),
            materialize_bounded_run(reads, current),
        );
    }
    Ok(machine)
}

fn materialize_bounded_run(
    reads: &MachineRunReadSet,
    current: &MachineRunCurrent,
) -> crate::RunProjection {
    let (scopes, open_scope_ids, open_scope_effects) = materialize_bounded_scopes(reads);
    let effects = reads
        .inputs
        .effects
        .iter()
        .filter_map(|(id, value)| value.clone().map(|value| (id.clone(), value)))
        .collect();
    let obligations = reads
        .inputs
        .obligations
        .iter()
        .filter_map(|(id, value)| value.clone().map(|value| (id.clone(), value)))
        .collect();
    let attempts = reads
        .inputs
        .attempts
        .iter()
        .filter_map(|(id, value)| value.clone().map(|value| (id.clone(), value)))
        .collect();
    let plan_lineage = if current.initial_plan == current.current_plan {
        vec![current.initial_plan.clone()]
    } else {
        vec![current.initial_plan.clone(), current.current_plan.clone()]
    };
    let binding_lineage = if current.initial_binding_context == current.current_binding_context {
        vec![current.initial_binding_context.clone()]
    } else {
        vec![
            current.initial_binding_context.clone(),
            current.current_binding_context.clone(),
        ]
    };
    crate::RunProjection {
        run_id: current.run_id.clone(),
        initial_plan: current.initial_plan.clone(),
        current_plan: current.current_plan.clone(),
        plan_lineage,
        initial_binding_context: current.initial_binding_context.clone(),
        current_binding_context: current.current_binding_context.clone(),
        binding_lineage,
        epoch: current.epoch,
        execution_status: current.execution_status.clone(),
        world_settlement: current.world_settlement,
        scopes,
        effects,
        obligations,
        attempts,
        result: current.result.clone(),
        last_event: current.last_event.clone(),
        derived: RunDerivedIndex {
            initialized: true,
            active_attempt: current.active_attempt_id.clone(),
            open_scope_ids,
            open_scope_effects,
            committed_effect_count: current.committed_effect_count,
            ..RunDerivedIndex::default()
        },
    }
}

fn materialize_bounded_scopes(
    reads: &MachineRunReadSet,
) -> (
    BTreeMap<String, crate::ScopeProjection>,
    BTreeSet<String>,
    BTreeMap<String, OpenScopeEffectIndex>,
) {
    let mut scopes = BTreeMap::new();
    let mut open_scope_ids = BTreeSet::new();
    let mut open_scope_effects = BTreeMap::new();
    for (scope_id, scope) in &reads.inputs.scopes {
        let Some(scope) = scope else {
            continue;
        };
        let (invocation_path, region_path) =
            reads.inputs.scope_locations.get(scope_id).map_or_else(
                || (Vec::new(), Vec::new()),
                |witness| (witness.invocation_path.clone(), witness.region_path.clone()),
            );
        let intents = reads
            .inputs
            .log_pages
            .iter()
            .find_map(|page| match page.selector() {
                MachineRunLogSelector::ScopeEffects { scope_id: selected }
                    if selected == scope_id =>
                {
                    Some(page.entries().to_vec())
                }
                _ => None,
            })
            .unwrap_or_default();
        if scope.status == crate::ScopeStatus::Open {
            open_scope_ids.insert(scope_id.clone());
            let mut index = OpenScopeEffectIndex {
                all_intents: intents.iter().cloned().collect(),
                ..OpenScopeEffectIndex::default()
            };
            index.all_intent_order.clone_from(&intents);
            if let Some(mutating) =
                reads
                    .inputs
                    .log_pages
                    .iter()
                    .find_map(|page| match page.selector() {
                        MachineRunLogSelector::ScopeMutatingEffects { scope_id: selected }
                            if selected == scope_id =>
                        {
                            Some(page.entries().to_vec())
                        }
                        _ => None,
                    })
            {
                index.mutating_intents = mutating.iter().cloned().collect();
                index.mutating_intent_order = mutating;
            }
            open_scope_effects.insert(scope_id.clone(), index);
        }
        scopes.insert(
            scope_id.clone(),
            crate::ScopeProjection {
                scope_id: scope.scope_id.clone(),
                parent_scope: scope.parent_scope.clone(),
                invocation_id: scope.invocation_id.clone(),
                invocation_path,
                definition_id: scope.definition_id.clone(),
                region_path,
                site_id: scope.site_id.clone(),
                status: scope.status,
                intents: intents.iter().cloned().collect(),
                intent_order: intents,
            },
        );
    }
    (scopes, open_scope_ids, open_scope_effects)
}

#[derive(Default)]
struct PinnedRunReduction {
    result_current: Option<MachineRunCurrent>,
    scopes: BTreeMap<String, MachineScopeCurrent>,
    effects: BTreeMap<String, crate::EffectProjection>,
    obligations: BTreeMap<String, ObligationProjection>,
    attempts: BTreeMap<String, crate::AttemptProjection>,
    indexes: Vec<MachineRunIndexMembershipDelta>,
    logs: Vec<MachineRunLogAppendDelta>,
    facts: BTreeMap<String, String>,
    expected_roots: BTreeMap<MachineRunRootUpdateTarget, u64>,
}

fn checked_result_count(parent: u64, inserted: usize, removed: usize) -> Result<u64> {
    let inserted =
        u64::try_from(inserted).map_err(|error| CoreError::Validation(error.to_string()))?;
    let removed =
        u64::try_from(removed).map_err(|error| CoreError::Validation(error.to_string()))?;
    parent
        .checked_add(inserted)
        .and_then(|value| value.checked_sub(removed))
        .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| {
            CoreError::Validation("pinned Machine root cardinality overflowed".to_owned())
        })
}

fn prepared_root_mutation(
    target: MachineRunRootUpdateTarget,
    parent: MachinePhysicalRoot,
    expected_count: u64,
    typed: MachineTypedRootMutation,
) -> Result<MachinePreparedRootMutation> {
    let mutation_digest =
        canonical_digest(&(PINNED_ROOT_MUTATION_DIGEST_DOMAIN, &target, &parent, &typed))?;
    Ok(MachinePreparedRootMutation {
        target,
        parent,
        expected_count,
        typed,
        mutation_digest,
    })
}

fn run_index_root_mut<'a>(
    current: &'a mut MachineRunCurrent,
    scopes: &'a mut BTreeMap<String, MachineScopeCurrent>,
    selector: &MachineRunIndexSelector,
) -> Result<&'a mut MachineMapRoot> {
    match selector {
        MachineRunIndexSelector::GovernanceEffects => Ok(&mut current.indexes.governance_effects),
        MachineRunIndexSelector::UnknownEffects => Ok(&mut current.indexes.unknown_effects),
        MachineRunIndexSelector::PendingEffects => Ok(&mut current.indexes.pending_effects),
        MachineRunIndexSelector::TerminalTransitionEffects => {
            Ok(&mut current.indexes.terminal_transition_effects)
        }
        MachineRunIndexSelector::OpenScopes => Ok(&mut current.indexes.open_scopes),
        MachineRunIndexSelector::UnresolvedObligations => {
            Ok(&mut current.indexes.unresolved_obligations)
        }
        MachineRunIndexSelector::ScopeEffects { scope_id }
        | MachineRunIndexSelector::ScopeMutatingEffects { scope_id }
        | MachineRunIndexSelector::ScopeAbortTransitions { scope_id }
        | MachineRunIndexSelector::ScopeAbortBlockers { scope_id } => {
            let scope =
                scopes
                    .get_mut(scope_id)
                    .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                        family: "result Machine Scope current",
                        key: scope_id.clone(),
                    })?;
            match selector {
                MachineRunIndexSelector::ScopeEffects { .. } => Ok(&mut scope.effects),
                MachineRunIndexSelector::ScopeMutatingEffects { .. } => {
                    Ok(&mut scope.mutating_effects)
                }
                MachineRunIndexSelector::ScopeAbortTransitions { .. } => {
                    Ok(&mut scope.abort_transitions)
                }
                MachineRunIndexSelector::ScopeAbortBlockers { .. } => Ok(&mut scope.abort_blockers),
                _ => unreachable!("scope selector was matched above"),
            }
        }
    }
}

fn run_log_root_mut<'a>(
    current: &'a mut MachineRunCurrent,
    scopes: &'a mut BTreeMap<String, MachineScopeCurrent>,
    selector: &MachineRunLogSelector,
) -> Result<&'a mut MachineLogRoot> {
    match selector {
        MachineRunLogSelector::Scopes => Ok(&mut current.order.scopes),
        MachineRunLogSelector::Effects => Ok(&mut current.order.effects),
        MachineRunLogSelector::Obligations => Ok(&mut current.order.obligations),
        MachineRunLogSelector::Attempts => Ok(&mut current.order.attempts),
        MachineRunLogSelector::Plans => Ok(&mut current.order.plans),
        MachineRunLogSelector::Bindings => Ok(&mut current.order.bindings),
        MachineRunLogSelector::ScopeEffects { scope_id }
        | MachineRunLogSelector::ScopeMutatingEffects { scope_id } => {
            let scope =
                scopes
                    .get_mut(scope_id)
                    .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                        family: "result Machine Scope current",
                        key: scope_id.clone(),
                    })?;
            match selector {
                MachineRunLogSelector::ScopeEffects { .. } => Ok(&mut scope.effect_order),
                MachineRunLogSelector::ScopeMutatingEffects { .. } => {
                    Ok(&mut scope.mutating_effect_order)
                }
                _ => unreachable!("scope log selector was matched above"),
            }
        }
    }
}

fn record_index_delta(
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    selector: MachineRunIndexSelector,
    inserted: BTreeSet<String>,
    removed: BTreeSet<String>,
) -> Result<()> {
    if inserted.is_empty() && removed.is_empty() {
        return Ok(());
    }
    if !inserted.is_disjoint(&removed) {
        return Err(CoreError::Validation(
            "pinned Machine index delta inserts and removes one identity".to_owned(),
        ));
    }
    let root = run_index_root_mut(current, &mut reduction.scopes, &selector)?;
    let result_count = checked_result_count(root.entries, inserted.len(), removed.len())?;
    root.entries = result_count;
    reduction.expected_roots.insert(
        MachineRunRootUpdateTarget::Index(selector.clone()),
        result_count,
    );
    reduction.indexes.push(MachineRunIndexMembershipDelta {
        selector,
        inserted,
        removed,
    });
    Ok(())
}

fn record_log_append(
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    selector: MachineRunLogSelector,
    values: Vec<String>,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    selector.validate_entries(&values)?;
    let root = run_log_root_mut(current, &mut reduction.scopes, &selector)?;
    let result_len = checked_result_count(root.len, values.len(), 0)?;
    root.len = result_len;
    match selector {
        MachineRunLogSelector::Plans => current.plan_lineage.len = result_len,
        MachineRunLogSelector::Bindings => current.binding_lineage.len = result_len,
        MachineRunLogSelector::Scopes
        | MachineRunLogSelector::Effects
        | MachineRunLogSelector::Obligations
        | MachineRunLogSelector::Attempts
        | MachineRunLogSelector::ScopeEffects { .. }
        | MachineRunLogSelector::ScopeMutatingEffects { .. } => {}
    }
    reduction.expected_roots.insert(
        MachineRunRootUpdateTarget::Log(selector.clone()),
        result_len,
    );
    reduction
        .logs
        .push(MachineRunLogAppendDelta { selector, values });
    Ok(())
}

fn record_child_map(
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    target: MachineRunRootUpdateTarget,
    inserted: usize,
    removed: usize,
) -> Result<()> {
    let root = match target {
        MachineRunRootUpdateTarget::Scopes => &mut current.children.scopes,
        MachineRunRootUpdateTarget::Effects => &mut current.children.effects,
        MachineRunRootUpdateTarget::Obligations => &mut current.children.obligations,
        MachineRunRootUpdateTarget::Attempts => &mut current.children.attempts,
        _ => {
            return Err(CoreError::Validation(
                "pinned Machine child-map target is not a Run child".to_owned(),
            ));
        }
    };
    let result_count = checked_result_count(root.entries, inserted, removed)?;
    root.entries = result_count;
    reduction.expected_roots.insert(target, result_count);
    Ok(())
}

fn settlement_selector(effect: &crate::EffectProjection) -> Option<MachineRunIndexSelector> {
    match crate::model::effect_settlement_class(effect) {
        crate::model::EffectSettlementClass::Governance => {
            Some(MachineRunIndexSelector::GovernanceEffects)
        }
        crate::model::EffectSettlementClass::Unknown => {
            Some(MachineRunIndexSelector::UnknownEffects)
        }
        crate::model::EffectSettlementClass::Pending => {
            Some(MachineRunIndexSelector::PendingEffects)
        }
        crate::model::EffectSettlementClass::Settled => None,
    }
}

fn transition_membership(
    previous: Option<&crate::EffectProjection>,
    next: Option<&crate::EffectProjection>,
    selector: MachineRunIndexSelector,
    predicate: impl Fn(&crate::EffectProjection) -> bool,
) -> Option<(MachineRunIndexSelector, BTreeSet<String>, BTreeSet<String>)> {
    let previous_member = previous.is_some_and(&predicate);
    let next_member = next.is_some_and(predicate);
    if previous_member == next_member {
        return None;
    }
    let identity = previous
        .or(next)
        .expect("membership transition has one Effect")
        .intent_id
        .clone();
    Some(if next_member {
        (selector, BTreeSet::from([identity]), BTreeSet::new())
    } else {
        (selector, BTreeSet::new(), BTreeSet::from([identity]))
    })
}

fn record_effect_index_transitions(
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    scope_status: crate::ScopeStatus,
    previous: Option<&crate::EffectProjection>,
    next: Option<&crate::EffectProjection>,
) -> Result<()> {
    let previous_settlement = previous.and_then(settlement_selector);
    let next_settlement = next.and_then(settlement_selector);
    if previous_settlement != next_settlement {
        if let Some(selector) = previous_settlement {
            let identity = previous
                .expect("previous selector has Effect")
                .intent_id
                .clone();
            record_index_delta(
                reduction,
                current,
                selector,
                BTreeSet::new(),
                BTreeSet::from([identity]),
            )?;
        }
        if let Some(selector) = next_settlement {
            let identity = next.expect("next selector has Effect").intent_id.clone();
            record_index_delta(
                reduction,
                current,
                selector,
                BTreeSet::from([identity]),
                BTreeSet::new(),
            )?;
        }
    }
    if let Some((selector, inserted, removed)) = transition_membership(
        previous,
        next,
        MachineRunIndexSelector::TerminalTransitionEffects,
        crate::model::needs_terminal_transition,
    ) {
        record_index_delta(reduction, current, selector, inserted, removed)?;
    }
    if scope_status == crate::ScopeStatus::Open {
        let scope_id = previous
            .or(next)
            .expect("Effect transition has one Effect")
            .scope_id
            .clone();
        if let Some((selector, inserted, removed)) = transition_membership(
            previous,
            next,
            MachineRunIndexSelector::ScopeAbortTransitions {
                scope_id: scope_id.clone(),
            },
            crate::model::needs_scope_abort_transition,
        ) {
            record_index_delta(reduction, current, selector, inserted, removed)?;
        }
        if let Some((selector, inserted, removed)) = transition_membership(
            previous,
            next,
            MachineRunIndexSelector::ScopeAbortBlockers { scope_id },
            crate::model::blocks_scope_abort,
        ) {
            record_index_delta(reduction, current, selector, inserted, removed)?;
        }
    }
    current.world_settlement = current.indexes.settlement();
    Ok(())
}

fn record_global_effect_index_transitions(
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    previous: &crate::EffectProjection,
    next: &crate::EffectProjection,
) -> Result<()> {
    let previous_settlement = settlement_selector(previous);
    let next_settlement = settlement_selector(next);
    if previous_settlement != next_settlement {
        if let Some(selector) = previous_settlement {
            record_index_delta(
                reduction,
                current,
                selector,
                BTreeSet::new(),
                BTreeSet::from([previous.intent_id.clone()]),
            )?;
        }
        if let Some(selector) = next_settlement {
            record_index_delta(
                reduction,
                current,
                selector,
                BTreeSet::from([next.intent_id.clone()]),
                BTreeSet::new(),
            )?;
        }
    }
    if let Some((selector, inserted, removed)) = transition_membership(
        Some(previous),
        Some(next),
        MachineRunIndexSelector::TerminalTransitionEffects,
        crate::model::needs_terminal_transition,
    ) {
        record_index_delta(reduction, current, selector, inserted, removed)?;
    }
    current.world_settlement = current.indexes.settlement();
    Ok(())
}

struct NewScopeCurrent<'a> {
    scope_id: String,
    parent_scope: Option<String>,
    invocation_id: String,
    invocation_path: &'a [crate::InvocationPathSegment],
    definition_id: String,
    region_path: &'a [usize],
    site_id: Option<String>,
    empty_map: &'a MachineMapRoot,
    empty_log: &'a MachineLogRoot,
}

fn new_scope_current(scope: NewScopeCurrent<'_>) -> Result<MachineScopeCurrent> {
    let NewScopeCurrent {
        scope_id,
        parent_scope,
        invocation_id,
        invocation_path,
        definition_id,
        region_path,
        site_id,
        empty_map,
        empty_log,
    } = scope;
    Ok(MachineScopeCurrent {
        scope_current_version: MACHINE_SCOPE_CURRENT_VERSION.to_owned(),
        scope_id,
        parent_scope,
        invocation_id,
        invocation_path_digest: canonical_digest(&invocation_path)?,
        definition_id,
        region_path_digest: canonical_digest(&region_path)?,
        site_id,
        status: crate::ScopeStatus::Open,
        effect_count: 0,
        direct_open_child_count: 0,
        effect_lineage_root: lineage_genesis(MACHINE_SCOPE_EFFECT_LINEAGE_DOMAIN)?,
        effects: empty_map.clone(),
        effect_order: empty_log.clone(),
        mutating_effect_lineage_root: lineage_genesis(
            MACHINE_SCOPE_MUTATING_EFFECT_LINEAGE_DOMAIN,
        )?,
        mutating_effects: empty_map.clone(),
        mutating_effect_order: empty_log.clone(),
        abort_transitions: empty_map.clone(),
        abort_blockers: empty_map.clone(),
    })
}

fn new_run_current(
    run_id: &str,
    plan_id: &str,
    binding_context: &str,
    event_id: &str,
    empty_map: &MachineMapRoot,
    empty_log: &MachineLogRoot,
) -> Result<MachineRunCurrent> {
    Ok(MachineRunCurrent {
        run_current_version: MACHINE_RUN_CURRENT_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        initial_plan: plan_id.to_owned(),
        current_plan: plan_id.to_owned(),
        plan_lineage_root: lineage_append(
            MACHINE_RUN_PLAN_LINEAGE_DOMAIN,
            &lineage_genesis(MACHINE_RUN_PLAN_LINEAGE_DOMAIN)?,
            plan_id,
        )?,
        plan_lineage_count: 1,
        plan_lineage: empty_log.clone(),
        initial_binding_context: binding_context.to_owned(),
        current_binding_context: binding_context.to_owned(),
        binding_lineage_root: lineage_append(
            MACHINE_RUN_BINDING_LINEAGE_DOMAIN,
            &lineage_genesis(MACHINE_RUN_BINDING_LINEAGE_DOMAIN)?,
            binding_context,
        )?,
        binding_lineage_count: 1,
        binding_lineage: empty_log.clone(),
        epoch: 0,
        execution_status: crate::RunExecutionStatus::Active,
        world_settlement: crate::WorldSettlementStatus::Settled,
        result: None,
        last_event: event_id.to_owned(),
        active_attempt_id: None,
        committed_effect_count: 0,
        reducer_state: MachineRunReducerState::Ready,
        children: MachineRunChildRoots {
            scopes: empty_map.clone(),
            effects: empty_map.clone(),
            obligations: empty_map.clone(),
            attempts: empty_map.clone(),
        },
        order: MachineRunOrderRoots {
            scopes: empty_log.clone(),
            effects: empty_log.clone(),
            obligations: empty_log.clone(),
            attempts: empty_log.clone(),
            plans: empty_log.clone(),
            bindings: empty_log.clone(),
        },
        indexes: MachineRunIndexRoots {
            governance_effects: empty_map.clone(),
            unknown_effects: empty_map.clone(),
            pending_effects: empty_map.clone(),
            terminal_transition_effects: empty_map.clone(),
            open_scopes: empty_map.clone(),
            unresolved_obligations: empty_map.clone(),
        },
    })
}

fn reduce_pinned_run_started(
    reads: &MachineRunReadSet,
    event: &Event,
    plan_id: &str,
    entry_definition: &str,
    binding_context: &str,
) -> Result<PinnedRunReduction> {
    let empty_map = reads.inputs.new_run_empty_root.as_ref().ok_or_else(|| {
        CoreError::PinnedReadSetIncomplete {
            family: "Machine empty child root",
            key: event.run_id.clone(),
        }
    })?;
    let empty_log = reads.inputs.new_run_empty_log.as_ref().ok_or_else(|| {
        CoreError::PinnedReadSetIncomplete {
            family: "Machine empty child log",
            key: event.run_id.clone(),
        }
    })?;
    let invocation_id = plan_invocation_id(&event.run_id, plan_id, entry_definition, &[])?;
    let root_scope = new_scope_current(NewScopeCurrent {
        scope_id: ROOT_SCOPE_ID.to_owned(),
        parent_scope: None,
        invocation_id,
        invocation_path: &[],
        definition_id: entry_definition.to_owned(),
        region_path: &[],
        site_id: None,
        empty_map,
        empty_log,
    })?;
    let mut reduction = PinnedRunReduction::default();
    reduction
        .scopes
        .insert(ROOT_SCOPE_ID.to_owned(), root_scope);
    let mut current = new_run_current(
        &event.run_id,
        plan_id,
        binding_context,
        &event.event_id,
        empty_map,
        empty_log,
    )?;
    record_child_map(
        &mut reduction,
        &mut current,
        MachineRunRootUpdateTarget::Scopes,
        1,
        0,
    )?;
    record_index_delta(
        &mut reduction,
        &mut current,
        MachineRunIndexSelector::OpenScopes,
        BTreeSet::from([ROOT_SCOPE_ID.to_owned()]),
        BTreeSet::new(),
    )?;
    for (selector, value) in [
        (MachineRunLogSelector::Scopes, ROOT_SCOPE_ID.to_owned()),
        (MachineRunLogSelector::Plans, plan_id.to_owned()),
        (MachineRunLogSelector::Bindings, binding_context.to_owned()),
    ] {
        record_log_append(&mut reduction, &mut current, selector, vec![value])?;
    }
    reduction.result_current = Some(current);
    Ok(reduction)
}

fn reduce_pinned_attempt_started(
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    payload: &EventPayload,
) -> Result<()> {
    let EventPayload::AttemptStarted {
        attempt_id,
        continuation_id,
        occurrence_binding,
        continuation_epoch,
        execution_fence,
    } = payload
    else {
        return Err(CoreError::Validation(
            "Attempt-start reducer received another Event payload".to_owned(),
        ));
    };
    if *continuation_epoch != current.epoch || current.active_attempt_id.is_some() {
        return Err(CoreError::IllegalTransition(format!(
            "Run {} cannot begin Attempt {attempt_id} at this epoch",
            current.run_id
        )));
    }
    let attempt = crate::AttemptProjection {
        attempt_id: attempt_id.clone(),
        continuation_id: continuation_id.clone(),
        occurrence_binding: occurrence_binding.clone(),
        continuation_epoch: *continuation_epoch,
        execution_fence: *execution_fence,
        active: true,
    };
    verify_attempt_read(&attempt)?;
    reduction.attempts.insert(attempt_id.clone(), attempt);
    current.active_attempt_id = Some(attempt_id.clone());
    record_child_map(
        reduction,
        current,
        MachineRunRootUpdateTarget::Attempts,
        1,
        0,
    )?;
    record_log_append(
        reduction,
        current,
        MachineRunLogSelector::Attempts,
        vec![attempt_id.clone()],
    )
}

fn reduce_pinned_attempt_yielded(
    reads: &MachineRunReadSet,
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    attempt_id: &str,
    continuation_epoch: u64,
    execution_fence: u64,
) -> Result<()> {
    let mut attempt = reads.require_attempt(attempt_id)?.clone();
    if current.active_attempt_id.as_deref() != Some(attempt_id)
        || !attempt.active
        || attempt.continuation_epoch != continuation_epoch
        || attempt.execution_fence != execution_fence
    {
        return Err(CoreError::IllegalTransition(format!(
            "attempt {attempt_id} is stale or inactive"
        )));
    }
    attempt.active = false;
    current.active_attempt_id = None;
    reduction.attempts.insert(attempt_id.to_owned(), attempt);
    record_child_map(
        reduction,
        current,
        MachineRunRootUpdateTarget::Attempts,
        0,
        0,
    )
}

fn reduce_pinned_epoch_advanced(
    reads: &MachineRunReadSet,
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    epoch: u64,
) -> Result<()> {
    let expected = current
        .epoch
        .checked_add(1)
        .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::IllegalTransition("Run epoch overflowed".to_owned()))?;
    if epoch != expected {
        return Err(CoreError::IllegalTransition(
            "Run epoch did not advance exactly once".to_owned(),
        ));
    }
    current.epoch = epoch;
    let Some(attempt_id) = current.active_attempt_id.take() else {
        return Ok(());
    };
    let mut attempt = reads.require_attempt(&attempt_id)?.clone();
    if !attempt.active {
        return Err(CoreError::Validation(
            "active Attempt scalar references an inactive leaf".to_owned(),
        ));
    }
    attempt.active = false;
    reduction.attempts.insert(attempt_id, attempt);
    record_child_map(
        reduction,
        current,
        MachineRunRootUpdateTarget::Attempts,
        0,
        0,
    )
}

fn reduce_pinned_scope_opened(
    reads: &MachineRunReadSet,
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    payload: &EventPayload,
) -> Result<()> {
    let EventPayload::ScopeOpened {
        scope_id,
        parent_scope,
        invocation_id,
        invocation_path,
        definition_id,
        region_path,
        site_id,
    } = payload
    else {
        return Err(CoreError::Validation(
            "Scope-open reducer received another Event payload".to_owned(),
        ));
    };
    let mut parent = reads.require_scope(parent_scope)?.clone();
    if parent.status != crate::ScopeStatus::Open {
        return Err(CoreError::IllegalTransition(format!(
            "parent scope {parent_scope} is not open"
        )));
    }
    parent.direct_open_child_count = parent
        .direct_open_child_count
        .checked_add(1)
        .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::Validation("direct open Scope count overflowed".to_owned()))?;
    let empty_map = reads.inputs.new_run_empty_root.as_ref().ok_or_else(|| {
        CoreError::PinnedReadSetIncomplete {
            family: "Machine empty child root",
            key: scope_id.clone(),
        }
    })?;
    let empty_log = reads.inputs.new_run_empty_log.as_ref().ok_or_else(|| {
        CoreError::PinnedReadSetIncomplete {
            family: "Machine empty child log",
            key: scope_id.clone(),
        }
    })?;
    let scope = new_scope_current(NewScopeCurrent {
        scope_id: scope_id.clone(),
        parent_scope: Some(parent_scope.clone()),
        invocation_id: invocation_id.clone(),
        invocation_path,
        definition_id: definition_id.clone(),
        region_path,
        site_id: Some(site_id.clone()),
        empty_map,
        empty_log,
    })?;
    reduction.scopes.insert(parent_scope.clone(), parent);
    reduction.scopes.insert(scope_id.clone(), scope);
    record_child_map(reduction, current, MachineRunRootUpdateTarget::Scopes, 1, 0)?;
    record_index_delta(
        reduction,
        current,
        MachineRunIndexSelector::OpenScopes,
        BTreeSet::from([scope_id.clone()]),
        BTreeSet::new(),
    )?;
    record_log_append(
        reduction,
        current,
        MachineRunLogSelector::Scopes,
        vec![scope_id.clone()],
    )
}

fn pinned_effect_from_payload(payload: &EventPayload) -> Result<crate::EffectProjection> {
    let EventPayload::EffectProposed {
        intent_id,
        origin_plan_id,
        scope_id,
        invocation_id,
        invocation_path,
        definition_id,
        region_path,
        site_id,
        occurrence,
        effect_schema_version,
        operation,
        profile,
        args,
        execution_binding,
        occurrence_binding,
    } = payload
    else {
        return Err(CoreError::Validation(
            "Effect-proposal reducer received another Event payload".to_owned(),
        ));
    };
    Ok(crate::EffectProjection {
        intent_id: intent_id.clone(),
        origin_plan_id: origin_plan_id.clone(),
        scope_id: scope_id.clone(),
        invocation_id: invocation_id.clone(),
        invocation_path: invocation_path.clone(),
        definition_id: definition_id.clone(),
        region_path: region_path.clone(),
        site_id: site_id.clone(),
        occurrence: occurrence.clone(),
        effect_schema_version: effect_schema_version.clone(),
        operation: operation.clone(),
        profile: profile.clone(),
        args: args.as_ref().clone(),
        execution_binding: execution_binding.as_ref().clone(),
        occurrence_binding: occurrence_binding.clone(),
        execution_availability: crate::EffectExecutionAvailability::Available,
        phase: crate::EffectPhase::Admitted,
        outcome: crate::WorldOutcome::Unobserved,
        reconciliation: crate::ReconciliationState::NotRequired,
    })
}

fn advance_scope_for_effect_proposal(
    scope: &mut MachineScopeCurrent,
    effect: &crate::EffectProjection,
) -> Result<()> {
    if scope.status != crate::ScopeStatus::Open {
        return Err(CoreError::IllegalTransition(format!(
            "scope {} is not open",
            scope.scope_id
        )));
    }
    scope.effect_count = scope
        .effect_count
        .checked_add(1)
        .ok_or_else(|| CoreError::Validation("Scope Effect count overflowed".to_owned()))?;
    scope.effect_lineage_root = lineage_append(
        MACHINE_SCOPE_EFFECT_LINEAGE_DOMAIN,
        &scope.effect_lineage_root,
        &effect.intent_id,
    )?;
    if effect.profile.mutation == crate::MutationKind::Mutating {
        scope.mutating_effect_lineage_root = lineage_append(
            MACHINE_SCOPE_MUTATING_EFFECT_LINEAGE_DOMAIN,
            &scope.mutating_effect_lineage_root,
            &effect.intent_id,
        )?;
    }
    Ok(())
}

fn record_pinned_effect_proposal_roots(
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    effect: &crate::EffectProjection,
) -> Result<()> {
    let scope_selector = MachineRunIndexSelector::ScopeEffects {
        scope_id: effect.scope_id.clone(),
    };
    let scope_log = MachineRunLogSelector::ScopeEffects {
        scope_id: effect.scope_id.clone(),
    };
    record_index_delta(
        reduction,
        current,
        scope_selector,
        BTreeSet::from([effect.intent_id.clone()]),
        BTreeSet::new(),
    )?;
    record_log_append(
        reduction,
        current,
        scope_log,
        vec![effect.intent_id.clone()],
    )?;
    if effect.profile.mutation == crate::MutationKind::Mutating {
        record_index_delta(
            reduction,
            current,
            MachineRunIndexSelector::ScopeMutatingEffects {
                scope_id: effect.scope_id.clone(),
            },
            BTreeSet::from([effect.intent_id.clone()]),
            BTreeSet::new(),
        )?;
        record_log_append(
            reduction,
            current,
            MachineRunLogSelector::ScopeMutatingEffects {
                scope_id: effect.scope_id.clone(),
            },
            vec![effect.intent_id.clone()],
        )?;
    }
    record_child_map(reduction, current, MachineRunRootUpdateTarget::Scopes, 0, 0)?;
    record_effect_index_transitions(
        reduction,
        current,
        crate::ScopeStatus::Open,
        None,
        Some(effect),
    )?;
    record_child_map(
        reduction,
        current,
        MachineRunRootUpdateTarget::Effects,
        1,
        0,
    )?;
    record_log_append(
        reduction,
        current,
        MachineRunLogSelector::Effects,
        vec![effect.intent_id.clone()],
    )
}

fn reduce_pinned_effect_proposed(
    reads: &MachineRunReadSet,
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    payload: &EventPayload,
) -> Result<()> {
    let effect = pinned_effect_from_payload(payload)?;
    verify_effect_read(&effect)?;
    let mut scope = reads.require_scope(&effect.scope_id)?.clone();
    advance_scope_for_effect_proposal(&mut scope, &effect)?;
    reduction.scopes.insert(effect.scope_id.clone(), scope);
    record_pinned_effect_proposal_roots(reduction, current, &effect)?;
    reduction.effects.insert(effect.intent_id.clone(), effect);
    Ok(())
}

fn update_pinned_effect_obligation(
    reads: &MachineRunReadSet,
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    effect: &crate::EffectProjection,
) -> Result<()> {
    if effect.profile.mutation != crate::MutationKind::Mutating {
        return Ok(());
    }
    let scope = reads.require_scope(&effect.scope_id)?;
    if scope.status != crate::ScopeStatus::ClosedCommitted {
        return Ok(());
    }
    let obligation_id = effect_obligation_id(&effect.intent_id)?;
    let mut obligation = reads
        .inputs
        .obligations
        .get(&obligation_id)
        .and_then(Option::as_ref)
        .cloned()
        .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
            family: "Machine obligation current",
            key: obligation_id.clone(),
        })?;
    let resolved = effect.phase == crate::EffectPhase::CancelledBeforeRelease
        || matches!(
            effect.outcome,
            crate::WorldOutcome::Applied | crate::WorldOutcome::NotApplied
        );
    if obligation.resolved == resolved {
        return Ok(());
    }
    let (inserted, removed) = if resolved {
        (BTreeSet::new(), BTreeSet::from([obligation_id.clone()]))
    } else {
        (BTreeSet::from([obligation_id.clone()]), BTreeSet::new())
    };
    record_index_delta(
        reduction,
        current,
        MachineRunIndexSelector::UnresolvedObligations,
        inserted,
        removed,
    )?;
    obligation.resolved = resolved;
    reduction.obligations.insert(obligation_id, obligation);
    record_child_map(
        reduction,
        current,
        MachineRunRootUpdateTarget::Obligations,
        0,
        0,
    )
}

fn reduce_pinned_effect_transitioned(
    reads: &MachineRunReadSet,
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    intent_id: &str,
    transition: &crate::EffectTransition,
) -> Result<()> {
    let previous = reads.require_effect(intent_id)?.clone();
    let scope = reads.require_scope(&previous.scope_id)?.clone();
    let mut next = previous.clone();
    crate::model::apply_effect_transition(&mut next, scope.status, transition)?;
    reduction
        .scopes
        .insert(scope.scope_id.clone(), scope.clone());
    record_effect_index_transitions(
        reduction,
        current,
        scope.status,
        Some(&previous),
        Some(&next),
    )?;
    if reduction.expected_roots.keys().any(is_scope_root_target) {
        record_child_map(reduction, current, MachineRunRootUpdateTarget::Scopes, 0, 0)?;
    } else {
        reduction.scopes.remove(&scope.scope_id);
    }
    reduction.effects.insert(intent_id.to_owned(), next.clone());
    record_child_map(
        reduction,
        current,
        MachineRunRootUpdateTarget::Effects,
        0,
        0,
    )?;
    update_pinned_effect_obligation(reads, reduction, current, &next)
}

fn reduce_pinned_binding_updated(
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    previous: &str,
    next: &str,
) -> Result<()> {
    if current.current_binding_context != previous {
        return Err(CoreError::IllegalTransition(
            "binding context changed from the expected value".to_owned(),
        ));
    }
    next.clone_into(&mut current.current_binding_context);
    current.binding_lineage_count = current
        .binding_lineage_count
        .checked_add(1)
        .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::Validation("binding lineage count overflowed".to_owned()))?;
    current.binding_lineage_root = lineage_append(
        MACHINE_RUN_BINDING_LINEAGE_DOMAIN,
        &current.binding_lineage_root,
        next,
    )?;
    record_log_append(
        reduction,
        current,
        MachineRunLogSelector::Bindings,
        vec![next.to_owned()],
    )
}

fn reduce_pinned_run_migrated(
    reduction: &mut PinnedRunReduction,
    current: &mut MachineRunCurrent,
    payload: &EventPayload,
) -> Result<()> {
    let EventPayload::RunMigrated {
        from_plan,
        to_plan,
        from_binding,
        to_binding,
        target_epoch,
        ..
    } = payload
    else {
        return Err(CoreError::Validation(
            "Run-migration reducer received another Event payload".to_owned(),
        ));
    };
    if current.current_plan != *from_plan
        || current.current_binding_context != *from_binding
        || current.active_attempt_id.is_some()
        || current.epoch.checked_add(1) != Some(*target_epoch)
    {
        return Err(CoreError::IllegalTransition(
            "Run migration does not match a quiescent current frontier".to_owned(),
        ));
    }
    to_plan.clone_into(&mut current.current_plan);
    to_binding.clone_into(&mut current.current_binding_context);
    current.epoch = *target_epoch;
    current.plan_lineage_count = current
        .plan_lineage_count
        .checked_add(1)
        .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::Validation("Plan lineage count overflowed".to_owned()))?;
    current.binding_lineage_count = current
        .binding_lineage_count
        .checked_add(1)
        .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::Validation("binding lineage count overflowed".to_owned()))?;
    current.plan_lineage_root = lineage_append(
        MACHINE_RUN_PLAN_LINEAGE_DOMAIN,
        &current.plan_lineage_root,
        to_plan,
    )?;
    current.binding_lineage_root = lineage_append(
        MACHINE_RUN_BINDING_LINEAGE_DOMAIN,
        &current.binding_lineage_root,
        to_binding,
    )?;
    record_log_append(
        reduction,
        current,
        MachineRunLogSelector::Plans,
        vec![to_plan.clone()],
    )?;
    record_log_append(
        reduction,
        current,
        MachineRunLogSelector::Bindings,
        vec![to_binding.clone()],
    )
}

fn reduce_pinned_fact_recorded(
    reads: &MachineRunReadSet,
    reduction: &mut PinnedRunReduction,
    frontier: &MachineAuthorityFrontier,
    key: &str,
    value: &str,
) -> Result<()> {
    match reads
        .inputs
        .facts
        .get(key)
        .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
            family: "Machine fact",
            key: key.to_owned(),
        })? {
        Some(existing) if existing != value => {
            return Err(CoreError::IllegalTransition(format!(
                "fact {key:?} already has a different value"
            )));
        }
        Some(_) => {}
        None => {
            reduction.expected_roots.insert(
                MachineRunRootUpdateTarget::Facts,
                checked_result_count(frontier.facts.entries, 1, 0)?,
            );
        }
    }
    reduction.facts.insert(key.to_owned(), value.to_owned());
    Ok(())
}

fn reduce_pinned_run_completed(
    current: &mut MachineRunCurrent,
    result: Option<&ArtifactRef>,
) -> Result<()> {
    if current.active_attempt_id.is_some()
        || current.indexes.unresolved_obligations.entries != 0
        || current.indexes.open_scopes.entries != 0
        || current.indexes.terminal_transition_effects.entries != 0
        || current.world_settlement != crate::WorldSettlementStatus::Settled
    {
        return Err(CoreError::IllegalTransition(
            "Run cannot complete while reducer work remains".to_owned(),
        ));
    }
    current.execution_status = crate::RunExecutionStatus::Completed;
    current.result = result.cloned();
    Ok(())
}

fn reduce_pinned_event(
    reads: &MachineRunReadSet,
    event: &Event,
    frontier: &MachineAuthorityFrontier,
) -> Result<PinnedRunReduction> {
    if matches!(
        event.payload,
        EventPayload::ScopeCommitted { .. } | EventPayload::ScopeAborted { .. }
    ) {
        return inline_scope::reduce_inline_scope(reads, event, frontier);
    }
    let mut reduction = PinnedRunReduction::default();
    if let EventPayload::RunStarted {
        plan_id,
        entry_definition,
        binding_context,
        ..
    } = &event.payload
    {
        return reduce_pinned_run_started(reads, event, plan_id, entry_definition, binding_context);
    }

    let mut current = reads
        .inputs
        .run
        .clone()
        .ok_or_else(|| CoreError::NotFound(format!("Run {} does not exist", event.run_id)))?;
    crate::model::verify_run_event_gate(&current.execution_status, &event.payload, &event.run_id)?;
    match &event.payload {
        EventPayload::RunStarted { .. } => unreachable!("RunStarted returned above"),
        EventPayload::AttemptStarted { .. } => {
            reduce_pinned_attempt_started(&mut reduction, &mut current, &event.payload)?;
        }
        EventPayload::AttemptYielded {
            attempt_id,
            continuation_epoch,
            execution_fence,
        } => {
            reduce_pinned_attempt_yielded(
                reads,
                &mut reduction,
                &mut current,
                attempt_id,
                *continuation_epoch,
                *execution_fence,
            )?;
        }
        EventPayload::EpochAdvanced { epoch } => {
            reduce_pinned_epoch_advanced(reads, &mut reduction, &mut current, *epoch)?;
        }
        EventPayload::ScopeOpened { .. } => {
            reduce_pinned_scope_opened(reads, &mut reduction, &mut current, &event.payload)?;
        }
        EventPayload::EffectProposed { .. } => {
            reduce_pinned_effect_proposed(reads, &mut reduction, &mut current, &event.payload)?;
        }
        EventPayload::EffectTransitioned {
            intent_id,
            transition,
        } => {
            reduce_pinned_effect_transitioned(
                reads,
                &mut reduction,
                &mut current,
                intent_id,
                transition,
            )?;
        }
        EventPayload::ScopeCommitted { .. }
        | EventPayload::ScopeAborted { .. }
        | EventPayload::RunFailed { .. }
        | EventPayload::RunCancelled { .. } => {
            return Err(CoreError::Validation(
                "unbounded Machine command requires the persisted page transition protocol"
                    .to_owned(),
            ));
        }
        EventPayload::BindingUpdated {
            previous,
            current: next,
        } => {
            reduce_pinned_binding_updated(&mut reduction, &mut current, previous, next)?;
        }
        EventPayload::RunMigrated { .. } => {
            reduce_pinned_run_migrated(&mut reduction, &mut current, &event.payload)?;
        }
        EventPayload::FactRecorded { key, value } => {
            reduce_pinned_fact_recorded(reads, &mut reduction, frontier, key, value)?;
        }
        EventPayload::RunCompleted { result } => {
            reduce_pinned_run_completed(&mut current, result.as_ref())?;
        }
    }
    current.last_event.clone_from(&event.event_id);
    reduction.result_current = Some(current);
    Ok(reduction)
}

fn build_machine_root_delta(
    parent: &MachineAuthorityFrontier,
    result: &MachineAuthorityFrontier,
    events: Vec<Event>,
    admission: CommandAdmission,
    record: &CommandRecord,
    index_proof: &MachineCommandIndexProof,
) -> MachineRootDelta {
    MachineRootDelta {
        root_delta_version: MachineRootDelta::VERSION.to_owned(),
        delta_version: MachineDelta::VERSION.to_owned(),
        parent_authority_root: parent.authority_root.clone(),
        result_authority_root: result.authority_root.clone(),
        parent_anchor_id: parent.base_anchor_id.clone(),
        result_anchor_id: result.base_anchor_id.clone(),
        plans: BTreeMap::new(),
        plan_admission_order: Vec::new(),
        artifacts: BTreeMap::new(),
        artifact_admission_order: Vec::new(),
        batches: BTreeMap::new(),
        batch_admission_order: Vec::new(),
        removed_event_ids: Vec::new(),
        removed_admission_ids: Vec::new(),
        removed_command_ids: BTreeSet::new(),
        removed_batch_ids: BTreeSet::new(),
        removed_command_index_proof_ids: BTreeSet::new(),
        base: None,
        base_anchor: None,
        archive_segment: None,
        events,
        admissions: vec![admission],
        commands: BTreeMap::from([(
            record.envelope.command_id.clone(),
            ArchivedCommandRecord::from_private(record),
        )]),
        command_index_proofs: BTreeMap::from([(
            record.envelope.command_id.clone(),
            index_proof.clone(),
        )]),
    }
}

#[derive(Default)]
struct MaterialInsertions {
    plans: BTreeMap<String, SealedPlan>,
    plan_order: Vec<String>,
    artifacts: BTreeMap<String, ArtifactRecord>,
    artifact_order: Vec<String>,
}

fn append_material_commitment(domain: &'static str, parent: &str, id: &str) -> Result<String> {
    crate::validate_content_id("Machine immutable admission parent", parent)?;
    crate::validate_content_id("Machine immutable admission identity", id)?;
    content_id(domain, &("append", parent, id))
}

fn append_batch_frontier(
    frontier: &MachineAuthorityFrontier,
    batch_id: &str,
) -> Result<MachineAuthorityFrontier> {
    let mut result = frontier.clone();
    result.batch_admission_commitment = append_material_commitment(
        MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN,
        &result.batch_admission_commitment,
        batch_id,
    )?;
    result.batch_count = result
        .batch_count
        .checked_add(1)
        .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::Validation("Machine batch count overflowed".to_owned()))?;
    result.authority_root = result.expected_authority_root()?;
    result.verify()?;
    Ok(result)
}

fn resolve_material_admission(
    frontier: &MachineAuthorityFrontier,
    material: &MachineMaterialAdmission,
    reads: &MachineMaterialParentReads,
) -> Result<(MaterialInsertions, MachineAuthorityFrontier)> {
    let plan_ids = material
        .plans
        .iter()
        .map(|plan| plan.plan_id.clone())
        .collect::<BTreeSet<_>>();
    let artifact_ids = material
        .artifacts
        .iter()
        .map(|artifact| artifact.reference.artifact_id.clone())
        .collect::<BTreeSet<_>>();
    if reads.plans.keys().cloned().collect::<BTreeSet<_>>() != plan_ids
        || reads.artifacts.keys().cloned().collect::<BTreeSet<_>>() != artifact_ids
    {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine material parent closure",
            key: material.material_digest.clone(),
        });
    }
    let mut inserted = MaterialInsertions::default();
    let mut result = frontier.clone();
    for plan in &material.plans {
        match reads.plans.get(&plan.plan_id) {
            Some(Some(existing)) if existing == plan => {}
            Some(None) => {
                inserted.plans.insert(plan.plan_id.clone(), plan.clone());
                inserted.plan_order.push(plan.plan_id.clone());
                result.plan_admission_commitment = append_material_commitment(
                    MACHINE_PLAN_ADMISSION_COMMITMENT_DOMAIN,
                    &result.plan_admission_commitment,
                    &plan.plan_id,
                )?;
                result.plan_count = result
                    .plan_count
                    .checked_add(1)
                    .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
                    .ok_or_else(|| {
                        CoreError::Validation("Machine Plan count overflowed".to_owned())
                    })?;
            }
            Some(Some(_)) => {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine Plan {} conflicts with retained immutable content",
                    plan.plan_id
                )));
            }
            None => unreachable!("exact Plan parent keys were compared above"),
        }
    }
    for artifact in &material.artifacts {
        let id = &artifact.reference.artifact_id;
        match reads.artifacts.get(id) {
            Some(Some(existing)) if existing == artifact => {}
            Some(None) => {
                inserted.artifacts.insert(id.clone(), artifact.clone());
                inserted.artifact_order.push(id.clone());
                result.artifact_admission_commitment = append_material_commitment(
                    MACHINE_ARTIFACT_ADMISSION_COMMITMENT_DOMAIN,
                    &result.artifact_admission_commitment,
                    id,
                )?;
                result.artifact_count = result
                    .artifact_count
                    .checked_add(1)
                    .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
                    .ok_or_else(|| {
                        CoreError::Validation("Machine Artifact count overflowed".to_owned())
                    })?;
            }
            Some(Some(_)) => {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine Artifact {id} conflicts with retained immutable content"
                )));
            }
            None => unreachable!("exact Artifact parent keys were compared above"),
        }
    }
    result.authority_root = result.expected_authority_root()?;
    result.verify()?;
    Ok((inserted, result))
}

/// Prepare one bounded material-only Machine transition for a framework-owned
/// command receipt committed by the same outer StateRoot CAS.
///
/// # Errors
///
/// Returns an error when parent reads are incomplete, retained immutable bytes
/// conflict, or the resulting Machine frontier cannot be derived exactly.
#[doc(hidden)]
pub fn prepare_machine_material_admission(
    frontier: &MachineAuthorityFrontier,
    material: &MachineMaterialAdmission,
    reads: &MachineMaterialParentReads,
) -> Result<PreparedMachineMaterialAdmission> {
    let mut prepared = prepare_material_delta(frontier, material, reads)?;
    let source = material.source_manifest();
    let batch_id = machine_command_batch_id(
        &frontier.authority_root,
        &[],
        Some(&material.material_digest),
        Some(&source),
        &source.plan_ids,
        &source.artifacts,
    )?;
    let result = append_batch_frontier(&prepared.frontier, &batch_id)?;
    let mut batch = MachineCommandBatchRecord {
        batch_version: MACHINE_COMMAND_BATCH_VERSION.to_owned(),
        batch_id,
        parent_authority_root: frontier.authority_root.clone(),
        admission_parent_authority_root: frontier.authority_root.clone(),
        members: Vec::new(),
        material_digest: Some(material.material_digest.clone()),
        material_source: Some(source.clone()),
        plan_ids: source.plan_ids,
        artifacts: source.artifacts,
        receipts: Vec::new(),
        event_ids: Vec::new(),
        result_authority_root: result.authority_root.clone(),
        batch_receipt_id: String::new(),
    };
    batch.batch_receipt_id = batch.expected_receipt_id()?;
    batch.verify()?;
    prepared
        .delta
        .result_authority_root
        .clone_from(&result.authority_root);
    prepared
        .delta
        .batch_admission_order
        .push(batch.batch_id.clone());
    prepared.delta.batches.insert(batch.batch_id.clone(), batch);
    prepared.frontier = result;
    Ok(prepared)
}

fn prepare_material_delta(
    frontier: &MachineAuthorityFrontier,
    material: &MachineMaterialAdmission,
    reads: &MachineMaterialParentReads,
) -> Result<PreparedMachineMaterialAdmission> {
    frontier.verify()?;
    account_material_inputs(material, reads, &mut 0)?;
    let (inserted, result) = resolve_material_admission(frontier, material, reads)?;
    let mut delta = MachineRootDelta {
        root_delta_version: MachineRootDelta::VERSION.to_owned(),
        delta_version: MachineDelta::VERSION.to_owned(),
        parent_authority_root: frontier.authority_root.clone(),
        result_authority_root: result.authority_root.clone(),
        parent_anchor_id: frontier.base_anchor_id.clone(),
        result_anchor_id: result.base_anchor_id.clone(),
        plans: BTreeMap::new(),
        plan_admission_order: Vec::new(),
        artifacts: BTreeMap::new(),
        artifact_admission_order: Vec::new(),
        batches: BTreeMap::new(),
        batch_admission_order: Vec::new(),
        removed_event_ids: Vec::new(),
        removed_admission_ids: Vec::new(),
        removed_command_ids: BTreeSet::new(),
        removed_batch_ids: BTreeSet::new(),
        removed_command_index_proof_ids: BTreeSet::new(),
        base: None,
        base_anchor: None,
        archive_segment: None,
        events: Vec::new(),
        admissions: Vec::new(),
        commands: BTreeMap::new(),
        command_index_proofs: BTreeMap::new(),
    };
    add_material_to_root_delta(&mut delta, inserted);
    Ok(PreparedMachineMaterialAdmission {
        source_command_id: material.source_command_id.clone(),
        material_digest: material.material_digest.clone(),
        frontier: result,
        delta,
    })
}

fn account_material_inputs(
    material: &MachineMaterialAdmission,
    reads: &MachineMaterialParentReads,
    total: &mut usize,
) -> Result<()> {
    account_material_leaves(material, total)?;
    for (key, value) in &reads.plans {
        account_material_parent_read("parent Machine Plan", key, value.as_ref(), total)?;
    }
    for (key, value) in &reads.artifacts {
        account_material_parent_read("parent Machine Artifact", key, value.as_ref(), total)?;
    }
    Ok(())
}

fn account_material_leaves(material: &MachineMaterialAdmission, total: &mut usize) -> Result<()> {
    for plan in &material.plans {
        account_read_bytes("proposed Machine Plan", plan, total)?;
    }
    for artifact in &material.artifacts {
        account_read_bytes("proposed Machine Artifact", artifact, total)?;
    }
    Ok(())
}

fn account_material_parent_read<T: serde::Serialize>(
    kind: &str,
    key: &str,
    value: Option<&T>,
    total: &mut usize,
) -> Result<()> {
    account_read_bytes("Machine material read key", &key, total)?;
    // A key/absence witness is not part of a present physical value's leaf.
    // Charge both once to the aggregate, without merging them into a fake leaf.
    account_read_bytes(kind, &value, total)
}

fn admit_start_run_material(
    frontier: &MachineAuthorityFrontier,
    reads: &MachineRunReadSet,
    envelope: &CommandEnvelope,
) -> Result<(MaterialInsertions, MachineAuthorityFrontier)> {
    let Command::StartRun {
        plan_id,
        binding_context,
        input,
        ..
    } = &envelope.command
    else {
        return Ok((MaterialInsertions::default(), frontier.clone()));
    };
    verify_start_run_material(envelope, &reads.inputs)?;
    let material =
        reads
            .inputs
            .start_material
            .as_ref()
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine StartRun material",
                key: envelope.run_id.clone(),
            })?;
    let parent_reads = MachineMaterialParentReads {
        plans: reads.inputs.plans.clone(),
        artifacts: reads.inputs.artifacts.clone(),
    };
    let (material_plan, execution_binding, input_record) = material.parts()?;
    if material_plan.plan_id != *plan_id
        || execution_binding.reference.artifact_id != *binding_context
        || input_record.reference.artifact_id != input.artifact_id
    {
        return Err(CoreError::IdentityMismatch(
            "StartRun material changed after read validation".to_owned(),
        ));
    }
    resolve_material_admission(frontier, &material.admission, &parent_reads)
}

fn add_material_to_root_delta(delta: &mut MachineRootDelta, admitted: MaterialInsertions) {
    delta.plans = admitted.plans;
    delta.plan_admission_order = admitted.plan_order;
    delta.artifacts = admitted.artifacts;
    delta.artifact_admission_order = admitted.artifact_order;
}

fn prepared_parent_root(
    frontier: &MachineAuthorityFrontier,
    reads: &MachineRunReadSet,
    target: &MachineRunRootUpdateTarget,
) -> Result<MachinePhysicalRoot> {
    let empty_map =
        || {
            reads.inputs.new_run_empty_root.clone().ok_or_else(|| {
                CoreError::PinnedReadSetIncomplete {
                    family: "Machine empty child root",
                    key: reads.inputs.run_id.clone(),
                }
            })
        };
    let current = reads.inputs.run.as_ref();
    let map = match target {
        MachineRunRootUpdateTarget::Runs => frontier.runs.clone(),
        MachineRunRootUpdateTarget::Facts => frontier.facts.clone(),
        MachineRunRootUpdateTarget::PendingCommands => frontier.pending_commands.clone(),
        MachineRunRootUpdateTarget::PagedTransitions => frontier.paged_transitions.clone(),
        MachineRunRootUpdateTarget::PagedMaterialPlans
        | MachineRunRootUpdateTarget::PagedMaterialArtifacts => {
            return Err(CoreError::Validation(
                "ordinary command cannot address staged material roots".to_owned(),
            ));
        }
        MachineRunRootUpdateTarget::Scopes => current
            .map(|run| run.children.scopes.clone())
            .map_or_else(empty_map, Ok)?,
        MachineRunRootUpdateTarget::Effects => current
            .map(|run| run.children.effects.clone())
            .map_or_else(empty_map, Ok)?,
        MachineRunRootUpdateTarget::Obligations => current
            .map(|run| run.children.obligations.clone())
            .map_or_else(empty_map, Ok)?,
        MachineRunRootUpdateTarget::Attempts => current
            .map(|run| run.children.attempts.clone())
            .map_or_else(empty_map, Ok)?,
        MachineRunRootUpdateTarget::Index(selector) => {
            let require_current = || {
                current.ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                    family: "Machine Run current",
                    key: reads.inputs.run_id.clone(),
                })
            };
            match selector {
                MachineRunIndexSelector::GovernanceEffects => {
                    require_current()?.indexes.governance_effects.clone()
                }
                MachineRunIndexSelector::UnknownEffects => {
                    require_current()?.indexes.unknown_effects.clone()
                }
                MachineRunIndexSelector::PendingEffects => {
                    require_current()?.indexes.pending_effects.clone()
                }
                MachineRunIndexSelector::TerminalTransitionEffects => require_current()?
                    .indexes
                    .terminal_transition_effects
                    .clone(),
                MachineRunIndexSelector::OpenScopes => current
                    .map(|run| run.indexes.open_scopes.clone())
                    .map_or_else(empty_map, Ok)?,
                MachineRunIndexSelector::UnresolvedObligations => {
                    require_current()?.indexes.unresolved_obligations.clone()
                }
                MachineRunIndexSelector::ScopeEffects { scope_id } => {
                    reads.require_scope(scope_id)?.effects.clone()
                }
                MachineRunIndexSelector::ScopeMutatingEffects { scope_id } => {
                    reads.require_scope(scope_id)?.mutating_effects.clone()
                }
                MachineRunIndexSelector::ScopeAbortTransitions { scope_id } => {
                    reads.require_scope(scope_id)?.abort_transitions.clone()
                }
                MachineRunIndexSelector::ScopeAbortBlockers { scope_id } => {
                    reads.require_scope(scope_id)?.abort_blockers.clone()
                }
            }
        }
        MachineRunRootUpdateTarget::Log(selector) => {
            return Ok(MachinePhysicalRoot::Log(prepared_parent_log_root(
                reads, selector,
            )?));
        }
    };
    Ok(MachinePhysicalRoot::Map(map))
}

fn prepared_parent_log_root(
    reads: &MachineRunReadSet,
    selector: &MachineRunLogSelector,
) -> Result<MachineLogRoot> {
    let current = reads.inputs.run.as_ref();
    let empty_log =
        || {
            reads.inputs.new_run_empty_log.clone().ok_or_else(|| {
                CoreError::PinnedReadSetIncomplete {
                    family: "Machine empty child log",
                    key: reads.inputs.run_id.clone(),
                }
            })
        };
    match selector {
        MachineRunLogSelector::Scopes => current
            .map(|run| run.order.scopes.clone())
            .map_or_else(empty_log, Ok),
        MachineRunLogSelector::Effects => current
            .map(|run| run.order.effects.clone())
            .map_or_else(empty_log, Ok),
        MachineRunLogSelector::Obligations => current
            .map(|run| run.order.obligations.clone())
            .map_or_else(empty_log, Ok),
        MachineRunLogSelector::Attempts => current
            .map(|run| run.order.attempts.clone())
            .map_or_else(empty_log, Ok),
        MachineRunLogSelector::Plans => current
            .map(|run| run.order.plans.clone())
            .map_or_else(empty_log, Ok),
        MachineRunLogSelector::Bindings => current
            .map(|run| run.order.bindings.clone())
            .map_or_else(empty_log, Ok),
        MachineRunLogSelector::ScopeEffects { scope_id } => {
            Ok(reads.require_scope(scope_id)?.effect_order.clone())
        }
        MachineRunLogSelector::ScopeMutatingEffects { scope_id } => {
            Ok(reads.require_scope(scope_id)?.mutating_effect_order.clone())
        }
    }
}

fn finalize_prepared_authority(prepared: &mut PreparedPinnedMachineTransition) -> Result<()> {
    prepared.local_authority = canonical_digest(&prepared.authority_preimage())?;
    Ok(())
}

fn prepare_conflict(
    frontier: &MachineAuthorityFrontier,
    index_proof: &MachineCommandIndexProof,
    envelope: CommandEnvelope,
    current_precondition: Option<String>,
) -> Result<PreparedPinnedMachineTransition> {
    let observed = envelope.expected_precondition.clone().ok_or_else(|| {
        CoreError::Validation("mutating commands require expected_precondition".to_owned())
    })?;
    let semantic_hash = canonical_digest(&envelope)?;
    let (batch_id, batch_position, batch_len) =
        single_command_batch_metadata(&frontier.authority_root, &envelope, &semantic_hash)?;
    let receipt = CommandReceipt {
        command_id: envelope.command_id.clone(),
        status: CommandReceiptStatus::Conflict,
        event_ids: Vec::new(),
        error_code: Some("stale_action".to_owned()),
        message: Some("the Run changed after the caller's view".to_owned()),
        observed_precondition: Some(observed),
        current_precondition,
    };
    let record = CommandRecord {
        envelope,
        semantic_hash,
        receipt: receipt.clone(),
        batch_id,
        batch_position,
        batch_len,
    };
    let admission = CommandAdmission::new(
        pinned_admission_parent(frontier)?,
        &record,
        frontier.projection_root.clone(),
        frontier.projection_root.clone(),
    )?;
    let mut result_frontier = frontier.clone();
    result_frontier.admission_sequence = admission.sequence;
    result_frontier.admission_head = Some(admission.admission_id.clone());
    result_frontier.authority_root = result_frontier.expected_authority_root()?;
    result_frontier.verify()?;
    let machine_delta = build_machine_root_delta(
        frontier,
        &result_frontier,
        Vec::new(),
        admission,
        &record,
        index_proof,
    );
    let mut prepared = PreparedPinnedMachineTransition {
        receipt,
        frontier: result_frontier,
        machine_delta,
        parent_current_digest: None,
        result_current: None,
        scopes: BTreeMap::new(),
        effects: BTreeMap::new(),
        obligations: BTreeMap::new(),
        attempts: BTreeMap::new(),
        indexes: Vec::new(),
        logs: Vec::new(),
        facts: BTreeMap::new(),
        expected_roots: BTreeMap::new(),
        parent_roots: BTreeMap::new(),
        local_authority: String::new(),
    };
    finalize_prepared_authority(&mut prepared)?;
    Ok(prepared)
}

fn build_pinned_events(
    reads: &MachineRunReadSet,
    envelope: &CommandEnvelope,
    semantic_hash: &str,
) -> Result<Vec<Event>> {
    let events = bounded_authority_machine(reads)?.command_event_batch(envelope, semantic_hash)?;
    for event in &events {
        event.verify()?;
        verify_event_footprint(event)?;
    }
    Ok(events)
}

fn reduce_pinned_event_batch(
    reads: &MachineRunReadSet,
    events: &[Event],
    frontier: &MachineAuthorityFrontier,
) -> Result<PinnedRunReduction> {
    let first = events.first().ok_or_else(|| {
        CoreError::Validation("applied command has an empty Event batch".to_owned())
    })?;
    let mut reduction = reduce_pinned_event(reads, first, frontier)?;
    if let Some(second) = events.get(1) {
        if events.len() != 2 || !matches!(first.payload, EventPayload::RunStarted { .. }) {
            return Err(CoreError::Validation(
                "only StartRun may produce a two-Event command batch".to_owned(),
            ));
        }
        let mut current = reduction.result_current.take().ok_or_else(|| {
            CoreError::Validation("StartRun Event batch lost its Run current".to_owned())
        })?;
        reduce_pinned_attempt_started(&mut reduction, &mut current, &second.payload)?;
        current.last_event.clone_from(&second.event_id);
        reduction.result_current = Some(current);
    }
    Ok(reduction)
}

fn admit_pinned_event_record(
    frontier: &MachineAuthorityFrontier,
    record: &CommandRecord,
    events: &[Event],
) -> Result<(CommandAdmission, MachineAuthorityFrontier)> {
    let mut after_projection = frontier.projection_root.clone();
    for event in events {
        after_projection = canonical_digest(&(
            PROJECTION_ROOT_EVENT_DOMAIN,
            after_projection.as_str(),
            event.event_id.as_str(),
        ))?;
    }
    let admission = CommandAdmission::new(
        pinned_admission_parent(frontier)?,
        record,
        frontier.projection_root.clone(),
        after_projection.clone(),
    )?;
    let mut result = frontier.clone();
    result.projection_root = after_projection;
    let event_count =
        u64::try_from(events.len()).map_err(|error| CoreError::Validation(error.to_string()))?;
    result.event_count = result
        .event_count
        .checked_add(event_count)
        .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::Validation("Machine Event count overflowed".to_owned()))?;
    result.admission_sequence = admission.sequence;
    result.admission_head = Some(admission.admission_id.clone());
    result.authority_root = result.expected_authority_root()?;
    Ok((admission, result))
}

fn prepare_applied(
    frontier: &MachineAuthorityFrontier,
    index_proof: &MachineCommandIndexProof,
    reads: &MachineRunReadSet,
    envelope: CommandEnvelope,
) -> Result<PreparedPinnedMachineTransition> {
    reads.require_command_reads(&envelope)?;
    if matches!(
        envelope.command,
        Command::CommitScope { .. }
            | Command::AbortScope { .. }
            | Command::FailRun { .. }
            | Command::CancelRun { .. }
    ) && reads.inline_scope.is_none()
    {
        return Err(CoreError::Validation(
            "unbounded Machine command must begin a persisted page transition".to_owned(),
        ));
    }
    let (material_admission, material_frontier) =
        admit_start_run_material(frontier, reads, &envelope)?;
    let semantic_hash = canonical_digest(&envelope)?;
    let (batch_id, batch_position, batch_len) =
        single_command_batch_metadata(&frontier.authority_root, &envelope, &semantic_hash)?;
    let events = build_pinned_events(reads, &envelope, &semantic_hash)?;
    let mut reduction = reduce_pinned_event_batch(reads, &events, frontier)?;
    let result_current = reduction.result_current.as_ref().ok_or_else(|| {
        CoreError::Validation("applied Event has no result Run current".to_owned())
    })?;
    let receipt = CommandReceipt {
        command_id: envelope.command_id.clone(),
        status: CommandReceiptStatus::Applied,
        event_ids: events.iter().map(|event| event.event_id.clone()).collect(),
        error_code: None,
        message: None,
        observed_precondition: envelope.expected_precondition.clone(),
        current_precondition: Some(result_current.precondition_token()),
    };
    let record = CommandRecord {
        envelope,
        semantic_hash,
        receipt: receipt.clone(),
        batch_id,
        batch_position,
        batch_len,
    };
    let (admission, result_frontier) =
        admit_pinned_event_record(&material_frontier, &record, &events)?;
    reduction.expected_roots.insert(
        MachineRunRootUpdateTarget::Runs,
        checked_result_count(
            frontier.runs.entries,
            usize::from(reads.inputs.run.is_none()),
            0,
        )?,
    );
    if !reduction.facts.is_empty() {
        reduction
            .expected_roots
            .entry(MachineRunRootUpdateTarget::Facts)
            .or_insert(frontier.facts.entries);
    }
    let parent_roots = reduction
        .expected_roots
        .keys()
        .map(|target| {
            prepared_parent_root(frontier, reads, target).map(|root| (target.clone(), root))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut machine_delta = build_machine_root_delta(
        frontier,
        &result_frontier,
        events,
        admission,
        &record,
        index_proof,
    );
    add_material_to_root_delta(&mut machine_delta, material_admission);
    let parent_current_digest = reads
        .inputs
        .run
        .as_ref()
        .map(canonical_digest)
        .transpose()?;
    let mut prepared = PreparedPinnedMachineTransition {
        receipt,
        frontier: result_frontier,
        machine_delta,
        parent_current_digest,
        result_current: reduction.result_current,
        scopes: reduction.scopes,
        effects: reduction.effects,
        obligations: reduction.obligations,
        attempts: reduction.attempts,
        indexes: reduction.indexes,
        logs: reduction.logs,
        facts: reduction.facts,
        expected_roots: reduction.expected_roots,
        parent_roots,
        local_authority: String::new(),
    };
    finalize_prepared_authority(&mut prepared)?;
    Ok(prepared)
}

fn paged_gate_payload(source: &MachineRunCurrent, command: &Command) -> Result<EventPayload> {
    match command {
        Command::CommitScope { scope_id } => Ok(EventPayload::ScopeCommitted {
            scope_id: scope_id.clone(),
            obligation_count: 0,
            obligation_commitment: crate::machine::scope_obligation_commitment_genesis()?,
        }),
        Command::AbortScope { scope_id } => Ok(EventPayload::ScopeAborted {
            scope_id: scope_id.clone(),
        }),
        Command::FailRun { failure } => Ok(EventPayload::RunFailed {
            failure: failure.clone(),
            epoch: source
                .epoch
                .checked_add(1)
                .filter(|epoch| *epoch <= crate::MAX_EXACT_INTEGER)
                .ok_or_else(|| {
                    CoreError::IllegalTransition(
                        "Run failure execution fence overflowed".to_owned(),
                    )
                })?,
        }),
        Command::CancelRun { reason } => Ok(EventPayload::RunCancelled {
            reason: reason.clone(),
            epoch: source
                .epoch
                .checked_add(1)
                .filter(|epoch| *epoch <= crate::MAX_EXACT_INTEGER)
                .ok_or_else(|| {
                    CoreError::IllegalTransition(
                        "Run cancellation execution fence overflowed".to_owned(),
                    )
                })?,
        }),
        _ => Err(CoreError::Validation(
            "ordinary Machine command cannot begin a paged transition".to_owned(),
        )),
    }
}

fn paged_action_sources(
    reads: &MachineRunReadSet,
    source: &MachineRunCurrent,
    command: &Command,
) -> Result<(MachinePagedTransitionAction, MachineLogRoot, MachineLogRoot)> {
    match command {
        Command::CommitScope { scope_id } => {
            let scope = reads.require_scope(scope_id)?;
            require_childless_open_scope(scope)?;
            Ok((
                MachinePagedTransitionAction::CommitScope {
                    scope_id: scope_id.clone(),
                },
                scope.mutating_effect_order.clone(),
                MachineLogRoot::empty(),
            ))
        }
        Command::AbortScope { scope_id } => {
            let scope = reads.require_scope(scope_id)?;
            require_childless_open_scope(scope)?;
            if scope.abort_blockers.entries != 0 {
                return Err(CoreError::IllegalTransition(format!(
                    "scope {scope_id} cannot abort after effect release"
                )));
            }
            Ok((
                MachinePagedTransitionAction::AbortScope {
                    scope_id: scope_id.clone(),
                },
                scope.effect_order.clone(),
                MachineLogRoot::empty(),
            ))
        }
        Command::FailRun { failure } => {
            failure.verify()?;
            Ok((
                MachinePagedTransitionAction::FailRun,
                source.order.effects.clone(),
                source.order.scopes.clone(),
            ))
        }
        Command::CancelRun { reason } => {
            reason.validate()?;
            Ok((
                MachinePagedTransitionAction::CancelRun,
                source.order.effects.clone(),
                source.order.scopes.clone(),
            ))
        }
        _ => Err(CoreError::Validation(
            "ordinary Machine command cannot begin a paged transition".to_owned(),
        )),
    }
}

fn require_childless_open_scope(scope: &MachineScopeCurrent) -> Result<()> {
    if scope.status != crate::ScopeStatus::Open || scope.direct_open_child_count != 0 {
        return Err(CoreError::IllegalTransition(format!(
            "scope {} is not a childless open scope",
            scope.scope_id
        )));
    }
    Ok(())
}

fn paged_begin_root_plans(
    frontier: &MachineAuthorityFrontier,
    source: &MachineRunCurrent,
    fenced_run: &MachineRunCurrent,
    transition: &MachinePagedTransitionCurrent,
) -> Result<Vec<MachinePreparedRootMutation>> {
    Ok(vec![
        prepared_root_mutation(
            MachineRunRootUpdateTarget::Runs,
            MachinePhysicalRoot::Map(frontier.runs.clone()),
            frontier.runs.entries,
            MachineTypedRootMutation::PutRuns(BTreeMap::from([(
                source.run_id.clone(),
                fenced_run.clone(),
            )])),
        )?,
        prepared_root_mutation(
            MachineRunRootUpdateTarget::PendingCommands,
            MachinePhysicalRoot::Map(frontier.pending_commands.clone()),
            checked_result_count(frontier.pending_commands.entries, 1, 0)?,
            MachineTypedRootMutation::ReserveCommand {
                command_id: transition.command_id.clone(),
                transition_id: transition.transition_id.clone(),
            },
        )?,
        prepared_root_mutation(
            MachineRunRootUpdateTarget::PagedTransitions,
            MachinePhysicalRoot::Map(frontier.paged_transitions.clone()),
            checked_result_count(frontier.paged_transitions.entries, 1, 0)?,
            MachineTypedRootMutation::PutPagedTransition(Box::new(transition.clone())),
        )?,
    ])
}

fn prepare_paged_transition_context(
    frontier: &MachineAuthorityFrontier,
    reads: &MachineRunReadSet,
    envelope: CommandEnvelope,
) -> Result<MachinePagedTransitionCurrent> {
    reads.require_command_reads(&envelope)?;
    let source =
        reads.inputs.run.as_ref().ok_or_else(|| {
            CoreError::NotFound(format!("Run {} does not exist", envelope.run_id))
        })?;
    let gate_payload = paged_gate_payload(source, &envelope.command)?;
    crate::model::verify_run_event_gate(&source.execution_status, &gate_payload, &source.run_id)?;
    if !matches!(source.reducer_state, MachineRunReducerState::Ready) {
        return Err(CoreError::IllegalTransition(format!(
            "Run {} is already owned by a paged transition",
            source.run_id
        )));
    }
    let (action, effect_source, scope_source) =
        paged_action_sources(reads, source, &envelope.command)?;
    let phase = if effect_source.len != 0 {
        MachinePagedTransitionPhase::Effects
    } else if scope_source.len != 0 {
        MachinePagedTransitionPhase::Scopes
    } else {
        MachinePagedTransitionPhase::Finalize
    };
    let command_hash = canonical_digest(&envelope)?;
    let command_id = envelope.command_id.clone();
    let source_run_current_digest = canonical_digest(source)?;
    let target_action_digest = content_id(MACHINE_PAGED_ACTION_ID_DOMAIN, &envelope.command)?;
    let batch_manifest = MachinePagedBatchManifest::from_command(frontier, &envelope)?;
    let mut transition = MachinePagedTransitionCurrent {
        transition_version: MACHINE_PAGED_TRANSITION_VERSION.to_owned(),
        transition_id: String::new(),
        envelope,
        batch_manifest,
        staged_material: MachinePagedMaterialRoots::empty(),
        command_id,
        command_hash,
        run_id: source.run_id.clone(),
        parent_revision: reads.inputs.machine_revision.clone(),
        source_run_current_digest: source_run_current_digest.clone(),
        action,
        target_action_digest,
        phase,
        effect_source,
        scope_source,
        next_index: 0,
        processed_count: 0,
        processed_commitment: lineage_genesis(MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN)?,
        obligation_count: 0,
        obligation_commitment: crate::machine::scope_obligation_commitment_genesis()?,
        shadow: MachinePagedShadowRoots {
            children: source.children.clone(),
            order: source.order.clone(),
            indexes: source.indexes.clone(),
        },
    };
    transition.transition_id = transition.expected_transition_id()?;
    transition.verify()?;
    Ok(transition)
}

fn prepare_paged_begin(
    frontier: &MachineAuthorityFrontier,
    reads: &MachineRunReadSet,
    envelope: CommandEnvelope,
) -> Result<PreparedPinnedPagedBegin> {
    let transition = prepare_paged_transition_context(frontier, reads, envelope)?;
    let source = reads
        .inputs
        .run
        .as_ref()
        .ok_or_else(|| CoreError::NotFound("paged source Run is absent".to_owned()))?;
    let source_run_current_digest = transition.source_run_current_digest.clone();
    let mut fenced_run = source.clone();
    fenced_run.reducer_state = MachineRunReducerState::Transitioning {
        transition_id: transition.transition_id.clone(),
    };
    fenced_run.verify()?;
    let plans = paged_begin_root_plans(frontier, source, &fenced_run, &transition)?;
    let mut prepared = PreparedPinnedPagedBegin {
        frontier: frontier.clone(),
        source_run_current_digest,
        fenced_run,
        transition,
        plans,
        local_authority: String::new(),
    };
    prepared.local_authority = canonical_digest(&(
        PREPARED_PAGED_BEGIN_AUTHORITY_DOMAIN,
        &prepared.frontier,
        &prepared.source_run_current_digest,
        &prepared.fenced_run,
        &prepared.transition,
        &prepared.plans,
    ))?;
    Ok(prepared)
}

fn verify_fenced_paged_run(
    transition: &MachinePagedTransitionCurrent,
    live_run: &MachineRunCurrent,
) -> Result<()> {
    live_run.verify()?;
    if live_run.run_id != transition.run_id
        || !matches!(
            &live_run.reducer_state,
            MachineRunReducerState::Transitioning { transition_id }
                if transition_id == &transition.transition_id
        )
    {
        return Err(CoreError::IdentityMismatch(
            "Machine paged transition does not own the exact live Run fence".to_owned(),
        ));
    }
    let mut source = live_run.clone();
    source.reducer_state = MachineRunReducerState::Ready;
    if canonical_digest(&source)? != transition.source_run_current_digest {
        return Err(CoreError::IdentityMismatch(
            "Machine paged transition source changed beneath its Run fence".to_owned(),
        ));
    }
    Ok(())
}

/// Return the unique exact source-log selector for the current paged phase.
///
/// # Errors
///
/// Returns an error when the transition is final or its action and phase are
/// inconsistent.
#[doc(hidden)]
pub fn pinned_paged_log_selector(
    transition: &MachinePagedTransitionCurrent,
) -> Result<MachineRunLogSelector> {
    match (&transition.action, transition.phase) {
        (
            MachinePagedTransitionAction::CommitScope { scope_id },
            MachinePagedTransitionPhase::Effects,
        ) => Ok(MachineRunLogSelector::ScopeMutatingEffects {
            scope_id: scope_id.clone(),
        }),
        (
            MachinePagedTransitionAction::AbortScope { scope_id },
            MachinePagedTransitionPhase::Effects,
        ) => Ok(MachineRunLogSelector::ScopeEffects {
            scope_id: scope_id.clone(),
        }),
        (
            MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun,
            MachinePagedTransitionPhase::Effects,
        ) => Ok(MachineRunLogSelector::Effects),
        (
            MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun,
            MachinePagedTransitionPhase::Scopes,
        ) => Ok(MachineRunLogSelector::Scopes),
        (_, MachinePagedTransitionPhase::Finalize) => Err(CoreError::IllegalTransition(
            "final Machine paged transition has no further source page".to_owned(),
        )),
        _ => Err(CoreError::Validation(
            "Machine paged transition phase does not match its action".to_owned(),
        )),
    }
}

fn paged_parent_root(
    transition: &MachinePagedTransitionCurrent,
    target: &MachineRunRootUpdateTarget,
) -> Result<MachinePhysicalRoot> {
    let root = match target {
        MachineRunRootUpdateTarget::Scopes => {
            MachinePhysicalRoot::Map(transition.shadow.children.scopes.clone())
        }
        MachineRunRootUpdateTarget::Effects => {
            MachinePhysicalRoot::Map(transition.shadow.children.effects.clone())
        }
        MachineRunRootUpdateTarget::Obligations => {
            MachinePhysicalRoot::Map(transition.shadow.children.obligations.clone())
        }
        MachineRunRootUpdateTarget::Attempts => {
            MachinePhysicalRoot::Map(transition.shadow.children.attempts.clone())
        }
        MachineRunRootUpdateTarget::Index(selector) => MachinePhysicalRoot::Map(match selector {
            MachineRunIndexSelector::GovernanceEffects => {
                transition.shadow.indexes.governance_effects.clone()
            }
            MachineRunIndexSelector::UnknownEffects => {
                transition.shadow.indexes.unknown_effects.clone()
            }
            MachineRunIndexSelector::PendingEffects => {
                transition.shadow.indexes.pending_effects.clone()
            }
            MachineRunIndexSelector::TerminalTransitionEffects => transition
                .shadow
                .indexes
                .terminal_transition_effects
                .clone(),
            MachineRunIndexSelector::OpenScopes => transition.shadow.indexes.open_scopes.clone(),
            MachineRunIndexSelector::UnresolvedObligations => {
                transition.shadow.indexes.unresolved_obligations.clone()
            }
            MachineRunIndexSelector::ScopeEffects { .. }
            | MachineRunIndexSelector::ScopeMutatingEffects { .. }
            | MachineRunIndexSelector::ScopeAbortTransitions { .. }
            | MachineRunIndexSelector::ScopeAbortBlockers { .. } => {
                return Err(CoreError::Validation(
                    "paged shadow step attempted a nested Scope root".to_owned(),
                ));
            }
        }),
        MachineRunRootUpdateTarget::Log(selector) => MachinePhysicalRoot::Log(match selector {
            MachineRunLogSelector::Scopes => transition.shadow.order.scopes.clone(),
            MachineRunLogSelector::Effects => transition.shadow.order.effects.clone(),
            MachineRunLogSelector::Obligations => transition.shadow.order.obligations.clone(),
            MachineRunLogSelector::Attempts => transition.shadow.order.attempts.clone(),
            MachineRunLogSelector::Plans => transition.shadow.order.plans.clone(),
            MachineRunLogSelector::Bindings => transition.shadow.order.bindings.clone(),
            MachineRunLogSelector::ScopeEffects { .. }
            | MachineRunLogSelector::ScopeMutatingEffects { .. } => {
                return Err(CoreError::Validation(
                    "paged shadow step attempted a nested Scope log".to_owned(),
                ));
            }
        }),
        MachineRunRootUpdateTarget::Runs
        | MachineRunRootUpdateTarget::Facts
        | MachineRunRootUpdateTarget::PendingCommands
        | MachineRunRootUpdateTarget::PagedTransitions
        | MachineRunRootUpdateTarget::PagedMaterialPlans
        | MachineRunRootUpdateTarget::PagedMaterialArtifacts => {
            return Err(CoreError::Validation(
                "paged shadow step attempted a global root".to_owned(),
            ));
        }
    };
    Ok(root)
}

fn paged_shadow_typed_mutation(
    reduction: &PinnedRunReduction,
    target: &MachineRunRootUpdateTarget,
) -> Result<MachineTypedRootMutation> {
    match target {
        MachineRunRootUpdateTarget::Scopes => Ok(MachineTypedRootMutation::PutScopes(
            reduction.scopes.clone(),
        )),
        MachineRunRootUpdateTarget::Effects => Ok(MachineTypedRootMutation::PutEffects(
            reduction.effects.clone(),
        )),
        MachineRunRootUpdateTarget::Obligations => Ok(MachineTypedRootMutation::PutObligations(
            reduction.obligations.clone(),
        )),
        MachineRunRootUpdateTarget::Attempts => Ok(MachineTypedRootMutation::PutAttempts(
            reduction.attempts.clone(),
        )),
        MachineRunRootUpdateTarget::Index(selector) => {
            Ok(MachineTypedRootMutation::UpdateMembership(
                reduction
                    .indexes
                    .iter()
                    .filter(|delta| &delta.selector == selector)
                    .cloned()
                    .collect(),
            ))
        }
        MachineRunRootUpdateTarget::Log(selector) => Ok(MachineTypedRootMutation::AppendLog(
            reduction
                .logs
                .iter()
                .filter(|delta| &delta.selector == selector)
                .cloned()
                .collect(),
        )),
        _ => Err(CoreError::Validation(
            "paged shadow mutation referenced an unsupported root".to_owned(),
        )),
    }
}

fn validate_paged_page_header(
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedReadInputs,
) -> Result<()> {
    verify_fenced_paged_run(transition, &inputs.live_run)?;
    inputs.page.verify_local()?;
    let selector = pinned_paged_log_selector(transition)?;
    let source = match transition.phase {
        MachinePagedTransitionPhase::Effects => &transition.effect_source,
        MachinePagedTransitionPhase::Scopes => &transition.scope_source,
        MachinePagedTransitionPhase::Finalize => {
            return Err(CoreError::IllegalTransition(
                "final Machine transition has no source page".to_owned(),
            ));
        }
    };
    if inputs.page.run_id != transition.run_id
        || inputs.page.selector != selector
        || inputs.page.source() != source
        || inputs.page.start() != transition.next_index
        || inputs.page.entries.is_empty()
    {
        return Err(CoreError::IdentityMismatch(
            "Machine paged input is not the exact next source page".to_owned(),
        ));
    }
    Ok(())
}

/// Resolve the one obligation leaf required by an Effect page entry.
///
/// # Errors
///
/// Returns an error when the Effect belongs to another Scope or the action and
/// owning Scope cannot legally require this entry.
#[doc(hidden)]
pub fn pinned_paged_obligation_read(
    transition: &MachinePagedTransitionCurrent,
    effect: &crate::EffectProjection,
    scope: &MachineScopeCurrent,
) -> Result<Option<String>> {
    if effect.scope_id != scope.scope_id {
        return Err(CoreError::IdentityMismatch(
            "paged Effect and owning Scope identities differ".to_owned(),
        ));
    }
    match &transition.action {
        MachinePagedTransitionAction::CommitScope { scope_id } => {
            if &effect.scope_id != scope_id
                || effect.profile.mutation != crate::MutationKind::Mutating
            {
                return Err(CoreError::IdentityMismatch(
                    "Scope commit page contains an invalid Effect".to_owned(),
                ));
            }
            Ok(Some(effect_obligation_id(&effect.intent_id)?))
        }
        MachinePagedTransitionAction::AbortScope { scope_id } => {
            if &effect.scope_id != scope_id {
                return Err(CoreError::IdentityMismatch(
                    "Scope abort page contains another Scope's Effect".to_owned(),
                ));
            }
            Ok(None)
        }
        MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun => {
            Ok((crate::model::needs_terminal_transition(effect)
                && scope.status == crate::ScopeStatus::ClosedCommitted
                && effect.profile.mutation == crate::MutationKind::Mutating)
                .then(|| effect_obligation_id(&effect.intent_id))
                .transpose()?)
        }
    }
}

fn expected_paged_read_keys(
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedReadInputs,
) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    let page_ids = inputs.page.entries.iter().cloned().collect::<BTreeSet<_>>();
    if transition.phase == MachinePagedTransitionPhase::Scopes {
        if !inputs.effects.is_empty() {
            return Err(CoreError::Validation(
                "Machine Scope page carried Effect leaves".to_owned(),
            ));
        }
        return Ok((page_ids, BTreeSet::new()));
    }
    if inputs.effects.keys().cloned().collect::<BTreeSet<_>>() != page_ids {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine paged Effect leaves",
            key: transition.transition_id.clone(),
        });
    }
    let mut scopes = BTreeSet::new();
    let mut obligations = BTreeSet::new();
    for (intent_id, effect) in &inputs.effects {
        verify_effect_read(effect)?;
        if effect.intent_id != *intent_id {
            return Err(CoreError::IdentityMismatch(
                "Machine paged Effect read changed identity".to_owned(),
            ));
        }
        scopes.insert(effect.scope_id.clone());
        let scope = inputs.scopes.get(&effect.scope_id).ok_or_else(|| {
            CoreError::PinnedReadSetIncomplete {
                family: "Machine paged owning Scope",
                key: effect.scope_id.clone(),
            }
        })?;
        if let Some(obligation) = pinned_paged_obligation_read(transition, effect, scope)? {
            obligations.insert(obligation);
        }
    }
    Ok((scopes, obligations))
}

fn validate_paged_read_leaves(
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedReadInputs,
    expected_scopes: &BTreeSet<String>,
    expected_obligations: &BTreeSet<String>,
) -> Result<()> {
    if inputs.scopes.keys().cloned().collect::<BTreeSet<_>>() != *expected_scopes {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine paged Scope leaves",
            key: transition.transition_id.clone(),
        });
    }
    for (scope_id, scope) in &inputs.scopes {
        scope.verify()?;
        if scope.scope_id != *scope_id {
            return Err(CoreError::IdentityMismatch(
                "Machine paged Scope read changed identity".to_owned(),
            ));
        }
    }
    if inputs.obligations.keys().cloned().collect::<BTreeSet<_>>() != *expected_obligations {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine paged obligation leaves",
            key: transition.transition_id.clone(),
        });
    }
    for (obligation_id, obligation) in &inputs.obligations {
        if let Some(obligation) = obligation {
            verify_obligation_read(obligation)?;
            if obligation.obligation_id != *obligation_id {
                return Err(CoreError::IdentityMismatch(
                    "Machine paged obligation read changed identity".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn account_paged_read_budget(
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedReadInputs,
) -> Result<()> {
    let mut read_bytes = 0;
    account_read_bytes(
        "Machine paged fixed authority",
        &(transition, &inputs.live_run, inputs.page.budget()),
        &mut read_bytes,
    )?;
    for value in &inputs.scopes {
        account_read_bytes("Machine paged Scope", &value, &mut read_bytes)?;
    }
    for value in &inputs.effects {
        account_read_bytes("Machine paged Effect", &value, &mut read_bytes)?;
    }
    for value in &inputs.obligations {
        account_read_bytes("Machine paged obligation", &value, &mut read_bytes)?;
    }
    Ok(())
}

fn process_paged_commit_effect(
    inputs: &MachinePagedReadInputs,
    reduction: &mut PinnedRunReduction,
    transition: &mut MachinePagedTransitionCurrent,
    effect: &crate::EffectProjection,
) -> Result<(String, bool)> {
    let obligation = crate::machine::obligation_for_effect(effect)?;
    let existing = inputs
        .obligations
        .get(&obligation.obligation_id)
        .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
            family: "Machine paged obligation leaf",
            key: obligation.obligation_id.clone(),
        })?;
    if existing.is_some() {
        return Err(CoreError::IllegalTransition(format!(
            "obligation {} already exists",
            obligation.obligation_id
        )));
    }
    transition.obligation_count = transition
        .obligation_count
        .checked_add(1)
        .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| {
            CoreError::Validation("paged Scope obligation count overflowed".to_owned())
        })?;
    transition.obligation_commitment = crate::machine::scope_obligation_commitment_append(
        &transition.obligation_commitment,
        &obligation,
    )?;
    let id = obligation.obligation_id.clone();
    let unresolved = !obligation.resolved;
    reduction.obligations.insert(id.clone(), obligation);
    Ok((id, unresolved))
}

fn process_paged_abort_effect(
    reduction: &mut PinnedRunReduction,
    shadow_run: &mut MachineRunCurrent,
    effect: &crate::EffectProjection,
) -> Result<String> {
    if !crate::model::needs_scope_abort_transition(effect) {
        return content_id(
            MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN,
            &("effect", &effect.intent_id, effect),
        );
    }
    let next = crate::model::terminalized_effect(effect);
    record_global_effect_index_transitions(reduction, shadow_run, effect, &next)?;
    reduction
        .effects
        .insert(effect.intent_id.clone(), next.clone());
    content_id(
        MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN,
        &("effect", &effect.intent_id, next),
    )
}

fn update_terminal_paged_obligation(
    inputs: &MachinePagedReadInputs,
    reduction: &mut PinnedRunReduction,
    shadow_run: &mut MachineRunCurrent,
    previous: &crate::EffectProjection,
    next: &crate::EffectProjection,
) -> Result<()> {
    if previous.profile.mutation != crate::MutationKind::Mutating {
        return Ok(());
    }
    let scope = inputs.scopes.get(&previous.scope_id).ok_or_else(|| {
        CoreError::PinnedReadSetIncomplete {
            family: "Machine paged owning Scope",
            key: previous.scope_id.clone(),
        }
    })?;
    if scope.status != crate::ScopeStatus::ClosedCommitted {
        return Ok(());
    }
    let id = effect_obligation_id(&previous.intent_id)?;
    let mut obligation = inputs
        .obligations
        .get(&id)
        .and_then(Option::as_ref)
        .cloned()
        .ok_or_else(|| CoreError::NotFound(format!("obligation {id} does not exist")))?;
    let resolved = next.phase == crate::EffectPhase::CancelledBeforeRelease
        || matches!(
            next.outcome,
            crate::WorldOutcome::Applied | crate::WorldOutcome::NotApplied
        );
    if obligation.resolved == resolved {
        return Ok(());
    }
    let (inserted, removed) = if resolved {
        (BTreeSet::new(), BTreeSet::from([id.clone()]))
    } else {
        (BTreeSet::from([id.clone()]), BTreeSet::new())
    };
    record_index_delta(
        reduction,
        shadow_run,
        MachineRunIndexSelector::UnresolvedObligations,
        inserted,
        removed,
    )?;
    obligation.resolved = resolved;
    reduction.obligations.insert(id, obligation);
    Ok(())
}

fn process_paged_terminal_effect(
    inputs: &MachinePagedReadInputs,
    reduction: &mut PinnedRunReduction,
    shadow_run: &mut MachineRunCurrent,
    effect: &crate::EffectProjection,
) -> Result<String> {
    if !crate::model::needs_terminal_transition(effect) {
        return content_id(
            MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN,
            &("effect", &effect.intent_id, effect),
        );
    }
    let next = crate::model::terminalized_effect(effect);
    record_global_effect_index_transitions(reduction, shadow_run, effect, &next)?;
    update_terminal_paged_obligation(inputs, reduction, shadow_run, effect, &next)?;
    reduction
        .effects
        .insert(effect.intent_id.clone(), next.clone());
    content_id(
        MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN,
        &("effect", &effect.intent_id, next),
    )
}

fn reduce_paged_effect_page(
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedReadInputs,
    reduction: &mut PinnedRunReduction,
    shadow_run: &mut MachineRunCurrent,
    next: &mut MachinePagedTransitionCurrent,
) -> Result<()> {
    let mut obligation_ids = Vec::new();
    let mut unresolved = BTreeSet::new();
    for intent_id in inputs.page.entries() {
        let effect =
            inputs
                .effects
                .get(intent_id)
                .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                    family: "Machine paged Effect leaf",
                    key: intent_id.clone(),
                })?;
        let processed = match transition.action {
            MachinePagedTransitionAction::CommitScope { .. } => {
                let (id, is_unresolved) =
                    process_paged_commit_effect(inputs, reduction, next, effect)?;
                obligation_ids.push(id.clone());
                if is_unresolved {
                    unresolved.insert(id.clone());
                }
                id
            }
            MachinePagedTransitionAction::AbortScope { .. } => {
                process_paged_abort_effect(reduction, shadow_run, effect)?
            }
            MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun => {
                process_paged_terminal_effect(inputs, reduction, shadow_run, effect)?
            }
        };
        next.processed_commitment = lineage_append(
            MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN,
            &next.processed_commitment,
            &processed,
        )?;
    }
    finalize_paged_effect_roots(
        transition,
        reduction,
        shadow_run,
        obligation_ids,
        unresolved,
    )
}

fn finalize_paged_effect_roots(
    transition: &MachinePagedTransitionCurrent,
    reduction: &mut PinnedRunReduction,
    shadow_run: &mut MachineRunCurrent,
    obligation_ids: Vec<String>,
    unresolved: BTreeSet<String>,
) -> Result<()> {
    if !reduction.effects.is_empty() {
        record_child_map(
            reduction,
            shadow_run,
            MachineRunRootUpdateTarget::Effects,
            0,
            0,
        )?;
    }
    if !reduction.obligations.is_empty() {
        let inserted = usize::from(matches!(
            transition.action,
            MachinePagedTransitionAction::CommitScope { .. }
        )) * reduction.obligations.len();
        record_child_map(
            reduction,
            shadow_run,
            MachineRunRootUpdateTarget::Obligations,
            inserted,
            0,
        )?;
    }
    if !obligation_ids.is_empty() {
        record_log_append(
            reduction,
            shadow_run,
            MachineRunLogSelector::Obligations,
            obligation_ids,
        )?;
    }
    if !unresolved.is_empty() {
        record_index_delta(
            reduction,
            shadow_run,
            MachineRunIndexSelector::UnresolvedObligations,
            unresolved,
            BTreeSet::new(),
        )?;
    }
    Ok(())
}

fn reduce_paged_scope_page(
    inputs: &MachinePagedReadInputs,
    reduction: &mut PinnedRunReduction,
    shadow_run: &mut MachineRunCurrent,
    next: &mut MachinePagedTransitionCurrent,
) -> Result<()> {
    let empty_map = MachineMapRoot::empty();
    let mut removed = BTreeSet::new();
    for scope_id in inputs.page.entries() {
        let scope =
            inputs
                .scopes
                .get(scope_id)
                .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                    family: "Machine paged Scope leaf",
                    key: scope_id.clone(),
                })?;
        let mut result = scope.clone();
        if result.status == crate::ScopeStatus::Open {
            result.status = crate::ScopeStatus::ClosedAborted;
            result.direct_open_child_count = 0;
            result.abort_transitions = empty_map.clone();
            result.abort_blockers = empty_map.clone();
            removed.insert(scope_id.clone());
            reduction.scopes.insert(scope_id.clone(), result.clone());
        }
        let processed = content_id(
            MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN,
            &("scope", scope_id, result),
        )?;
        next.processed_commitment = lineage_append(
            MACHINE_PAGED_PROCESSED_LINEAGE_DOMAIN,
            &next.processed_commitment,
            &processed,
        )?;
    }
    if !reduction.scopes.is_empty() {
        record_child_map(
            reduction,
            shadow_run,
            MachineRunRootUpdateTarget::Scopes,
            0,
            0,
        )?;
    }
    if !removed.is_empty() {
        record_index_delta(
            reduction,
            shadow_run,
            MachineRunIndexSelector::OpenScopes,
            BTreeSet::new(),
            removed,
        )?;
    }
    Ok(())
}

/// Prepare one exact bounded page of a persisted transition.
#[doc(hidden)]
pub fn prepare_pinned_transition_page(
    frontier: &MachineAuthorityFrontier,
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedReadInputs,
) -> Result<PreparedPinnedPagedStep> {
    frontier.verify()?;
    transition.verify()?;
    validate_paged_page_header(transition, inputs)?;
    let (expected_scopes, expected_obligations) = expected_paged_read_keys(transition, inputs)?;
    validate_paged_read_leaves(transition, inputs, &expected_scopes, &expected_obligations)?;
    account_paged_read_budget(transition, inputs)?;

    let mut shadow_run = inputs.live_run.clone();
    shadow_run.children = transition.shadow.children.clone();
    shadow_run.order = transition.shadow.order.clone();
    shadow_run.indexes = transition.shadow.indexes.clone();
    let mut reduction = PinnedRunReduction::default();
    let mut next_transition = transition.clone();
    match transition.phase {
        MachinePagedTransitionPhase::Effects => {
            reduce_paged_effect_page(
                transition,
                inputs,
                &mut reduction,
                &mut shadow_run,
                &mut next_transition,
            )?;
        }
        MachinePagedTransitionPhase::Scopes => {
            reduce_paged_scope_page(
                inputs,
                &mut reduction,
                &mut shadow_run,
                &mut next_transition,
            )?;
        }
        MachinePagedTransitionPhase::Finalize => {
            return Err(CoreError::IllegalTransition(
                "final Machine transition has no source page".to_owned(),
            ));
        }
    }
    let page_len = u64::try_from(inputs.page.entries.len())
        .map_err(|error| CoreError::Validation(error.to_string()))?;
    next_transition.processed_count = next_transition
        .processed_count
        .checked_add(page_len)
        .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::Validation("paged processed count overflowed".to_owned()))?;
    if inputs.page.is_terminal()? {
        if transition.phase == MachinePagedTransitionPhase::Effects
            && matches!(
                transition.action,
                MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun
            )
            && transition.scope_source.len != 0
        {
            next_transition.phase = MachinePagedTransitionPhase::Scopes;
            next_transition.next_index = 0;
        } else {
            next_transition.phase = MachinePagedTransitionPhase::Finalize;
            next_transition.next_index = 0;
        }
    } else {
        next_transition.next_index = inputs.page.end()?;
    }
    next_transition.shadow.children = shadow_run.children.clone();
    next_transition.shadow.order = shadow_run.order.clone();
    next_transition.shadow.indexes = shadow_run.indexes.clone();
    let plans = paged_shadow_root_plans(transition, &reduction)?;
    let mut prepared = PreparedPinnedPagedStep {
        frontier: frontier.clone(),
        parent_transition: transition.clone(),
        transition: next_transition,
        shadow_run,
        reduction,
        plans,
        local_authority: String::new(),
    };
    prepared.refresh_local_authority()?;
    Ok(prepared)
}

fn expected_final_scope_keys(
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedFinalizeInputs,
) -> Result<BTreeSet<String>> {
    let mut expected = BTreeSet::new();
    let scope_id = match &transition.action {
        MachinePagedTransitionAction::CommitScope { scope_id }
        | MachinePagedTransitionAction::AbortScope { scope_id } => scope_id,
        MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun => {
            return Ok(expected);
        }
    };
    expected.insert(scope_id.clone());
    let target = inputs
        .scopes
        .get(scope_id)
        .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
            family: "Machine paged final Scope",
            key: scope_id.clone(),
        })?;
    require_childless_open_scope(target)?;
    if let Some(parent) = &target.parent_scope {
        expected.insert(parent.clone());
    }
    match transition.action {
        MachinePagedTransitionAction::CommitScope { .. }
            if target.mutating_effect_order != transition.effect_source
                || transition.obligation_count != target.mutating_effects.entries =>
        {
            return Err(CoreError::IdentityMismatch(
                "Scope commit accumulator does not cover its exact mutating Effect source"
                    .to_owned(),
            ));
        }
        MachinePagedTransitionAction::AbortScope { .. }
            if target.effect_order != transition.effect_source =>
        {
            return Err(CoreError::IdentityMismatch(
                "Scope abort accumulator does not cover its exact Effect source".to_owned(),
            ));
        }
        _ => {}
    }
    Ok(expected)
}

fn validate_final_attempt(
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedFinalizeInputs,
) -> Result<()> {
    if matches!(
        transition.action,
        MachinePagedTransitionAction::CommitScope { .. }
            | MachinePagedTransitionAction::AbortScope { .. }
    ) {
        if inputs.active_attempt.is_some() {
            return Err(CoreError::Validation(
                "Scope finalization carried an unrelated active Attempt leaf".to_owned(),
            ));
        }
        return Ok(());
    }
    match (&inputs.live_run.active_attempt_id, &inputs.active_attempt) {
        (Some(expected), Some(attempt)) if &attempt.attempt_id == expected && attempt.active => {
            verify_attempt_read(attempt)?;
        }
        (None, None) => {}
        _ => {
            return Err(CoreError::PinnedReadSetIncomplete {
                family: "Machine paged final active Attempt",
                key: transition.run_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_paged_finalize_inputs(
    frontier: &MachineAuthorityFrontier,
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedFinalizeInputs,
) -> Result<()> {
    if transition.phase != MachinePagedTransitionPhase::Finalize {
        return Err(CoreError::IllegalTransition(
            "Machine paged transition has unprocessed source pages".to_owned(),
        ));
    }
    verify_fenced_paged_run(transition, &inputs.live_run)?;
    if inputs.command_index_proof.command_id != transition.command_id
        || inputs.command_index_proof.value.is_some()
    {
        return Err(CoreError::IdentityMismatch(
            "paged finalization has the wrong current command non-membership proof".to_owned(),
        ));
    }
    inputs
        .command_index_proof
        .verify(&frontier.command_index_root)?;
    let expected = expected_final_scope_keys(transition, inputs)?;
    if inputs.scopes.keys().cloned().collect::<BTreeSet<_>>() != expected {
        return Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine paged final Scope closure",
            key: transition.transition_id.clone(),
        });
    }
    for (scope_id, scope) in &inputs.scopes {
        scope.verify()?;
        if &scope.scope_id != scope_id {
            return Err(CoreError::IdentityMismatch(
                "Machine paged final Scope changed identity".to_owned(),
            ));
        }
    }
    validate_final_attempt(transition, inputs)
}

fn close_paged_scope_current(
    scopes: &BTreeMap<String, MachineScopeCurrent>,
    reduction: &mut PinnedRunReduction,
    result: &mut MachineRunCurrent,
    scope_id: &str,
    status: crate::ScopeStatus,
) -> Result<()> {
    let mut target =
        scopes
            .get(scope_id)
            .cloned()
            .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                family: "Machine paged final Scope",
                key: scope_id.to_owned(),
            })?;
    target.status = status;
    target.abort_transitions = MachineMapRoot::empty();
    target.abort_blockers = MachineMapRoot::empty();
    if let Some(parent_id) = &target.parent_scope {
        let mut parent =
            scopes
                .get(parent_id)
                .cloned()
                .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                    family: "Machine paged final parent Scope",
                    key: parent_id.clone(),
                })?;
        parent.direct_open_child_count =
            parent
                .direct_open_child_count
                .checked_sub(1)
                .ok_or_else(|| {
                    CoreError::Validation("parent Scope direct-open count underflowed".to_owned())
                })?;
        reduction.scopes.insert(parent_id.clone(), parent);
    }
    if status == crate::ScopeStatus::ClosedCommitted {
        result.committed_effect_count = result
            .committed_effect_count
            .checked_add(target.effect_count)
            .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| CoreError::Validation("committed Effect count overflowed".to_owned()))?;
    }
    reduction.scopes.insert(scope_id.to_owned(), target);
    record_child_map(reduction, result, MachineRunRootUpdateTarget::Scopes, 0, 0)?;
    record_index_delta(
        reduction,
        result,
        MachineRunIndexSelector::OpenScopes,
        BTreeSet::new(),
        BTreeSet::from([scope_id.to_owned()]),
    )?;
    Ok(())
}

fn finalize_paged_scope_action(
    transition: &MachinePagedTransitionCurrent,
    scopes: &BTreeMap<String, MachineScopeCurrent>,
    reduction: &mut PinnedRunReduction,
    result: &mut MachineRunCurrent,
) -> Result<EventPayload> {
    match &transition.action {
        MachinePagedTransitionAction::CommitScope { scope_id } => {
            close_paged_scope_current(
                scopes,
                reduction,
                result,
                scope_id,
                crate::ScopeStatus::ClosedCommitted,
            )?;
            Ok(EventPayload::ScopeCommitted {
                scope_id: scope_id.clone(),
                obligation_count: transition.obligation_count,
                obligation_commitment: transition.obligation_commitment.clone(),
            })
        }
        MachinePagedTransitionAction::AbortScope { scope_id } => {
            close_paged_scope_current(
                scopes,
                reduction,
                result,
                scope_id,
                crate::ScopeStatus::ClosedAborted,
            )?;
            Ok(EventPayload::ScopeAborted {
                scope_id: scope_id.clone(),
            })
        }
        _ => Err(CoreError::Validation(
            "Run-terminal action cannot finalize a Scope".to_owned(),
        )),
    }
}

fn finalize_paged_run_action(
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedFinalizeInputs,
    reduction: &mut PinnedRunReduction,
    result: &mut MachineRunCurrent,
) -> Result<EventPayload> {
    if let Some(mut attempt) = inputs.active_attempt.clone() {
        attempt.active = false;
        reduction
            .attempts
            .insert(attempt.attempt_id.clone(), attempt);
        record_child_map(
            reduction,
            result,
            MachineRunRootUpdateTarget::Attempts,
            0,
            0,
        )?;
    }
    result.active_attempt_id = None;
    result.epoch = result
        .epoch
        .checked_add(1)
        .filter(|epoch| *epoch <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::IllegalTransition("Run execution fence overflowed".to_owned()))?;
    match (&transition.action, &transition.envelope.command) {
        (MachinePagedTransitionAction::FailRun, Command::FailRun { failure }) => {
            result.execution_status = crate::RunExecutionStatus::Failed {
                failure: failure.clone(),
            };
            Ok(EventPayload::RunFailed {
                failure: failure.clone(),
                epoch: result.epoch,
            })
        }
        (MachinePagedTransitionAction::CancelRun, Command::CancelRun { reason }) => {
            result.execution_status = crate::RunExecutionStatus::Cancelled {
                reason: reason.clone(),
            };
            Ok(EventPayload::RunCancelled {
                reason: reason.clone(),
                epoch: result.epoch,
            })
        }
        _ => Err(CoreError::IdentityMismatch(
            "paged Run action does not match its exact command".to_owned(),
        )),
    }
}

fn build_paged_final_event(
    transition: &MachinePagedTransitionCurrent,
    parent_event: &str,
    payload: EventPayload,
) -> Result<Event> {
    let (reads, writes, coordination_key) = footprints(&transition.run_id, &payload);
    let event = Event::new(EventContent {
        command_id: transition.command_id.clone(),
        command_hash: transition.command_hash.clone(),
        run_id: transition.run_id.clone(),
        parents: vec![parent_event.to_owned()],
        reads,
        writes,
        coordination_key,
        payload,
    })?;
    event.verify()?;
    verify_event_footprint(&event)?;
    Ok(event)
}

fn paged_shadow_root_plans(
    transition: &MachinePagedTransitionCurrent,
    reduction: &PinnedRunReduction,
) -> Result<Vec<MachinePreparedRootMutation>> {
    reduction
        .expected_roots
        .iter()
        .map(|(target, expected_count)| {
            let parent = paged_parent_root(transition, target)?;
            prepared_root_mutation(
                target.clone(),
                parent,
                *expected_count,
                paged_shadow_typed_mutation(reduction, target)?,
            )
        })
        .collect()
}

fn admit_paged_final_material(
    frontier: &MachineAuthorityFrontier,
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedFinalizeInputs,
) -> Result<(MaterialInsertions, MachineAuthorityFrontier)> {
    let Some((material, reads)) = &inputs.material else {
        if transition.batch_manifest.material_digest.is_some() {
            return Err(CoreError::NotFound(
                "paged finalization is missing its staged material".to_owned(),
            ));
        }
        return Ok((MaterialInsertions::default(), frontier.clone()));
    };
    if Some(material.material_digest.as_str())
        != transition.batch_manifest.material_digest.as_deref()
        || transition.batch_manifest.material_source.as_ref() != Some(&material.source_manifest())
        || u64::try_from(material.plans.len()).ok()
            != Some(transition.staged_material.plans.entries)
        || u64::try_from(material.artifacts.len()).ok()
            != Some(transition.staged_material.artifacts.entries)
    {
        return Err(CoreError::IdentityMismatch(
            "paged finalization changed its staged material closure".to_owned(),
        ));
    }
    let (mut plan_ids, mut artifacts) = command_material_membership(&transition.envelope.command)?;
    plan_ids.extend(material.plans.iter().map(|plan| plan.plan_id.clone()));
    artifacts.extend(
        material
            .artifacts
            .iter()
            .map(|artifact| artifact.reference.clone()),
    );
    plan_ids.sort();
    plan_ids.dedup();
    artifacts.sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
    artifacts.dedup();
    if transition.batch_manifest.plan_ids != plan_ids
        || transition.batch_manifest.artifacts != artifacts
    {
        return Err(CoreError::IdentityMismatch(
            "paged finalization changed batch material membership".to_owned(),
        ));
    }
    let mut total = 0;
    account_read_bytes("paged final Run", &inputs.live_run, &mut total)?;
    for scope in inputs.scopes.values() {
        account_read_bytes("paged final Scope", scope, &mut total)?;
    }
    account_material_inputs(material, reads, &mut total)?;
    resolve_material_admission(frontier, material, reads)
}

fn admit_paged_final_batch(
    frontier: &MachineAuthorityFrontier,
    transition: &MachinePagedTransitionCurrent,
    inputs: &MachinePagedFinalizeInputs,
    receipt: &CommandReceipt,
    event: Event,
) -> Result<(MachineRootDelta, MachineAuthorityFrontier)> {
    let (material, mut batch_frontier) = admit_paged_final_material(frontier, transition, inputs)?;
    batch_frontier.batch_admission_commitment = append_material_commitment(
        MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN,
        &batch_frontier.batch_admission_commitment,
        &transition.batch_manifest.batch_id,
    )?;
    batch_frontier.batch_count = batch_frontier
        .batch_count
        .checked_add(1)
        .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::Validation("paged batch count overflowed".to_owned()))?;
    batch_frontier.authority_root = batch_frontier.expected_authority_root()?;
    batch_frontier.verify()?;
    let record = CommandRecord {
        envelope: transition.envelope.clone(),
        semantic_hash: transition.command_hash.clone(),
        receipt: receipt.clone(),
        batch_id: transition.batch_manifest.batch_id.clone(),
        batch_position: 0,
        batch_len: 1,
    };
    let (admission, result) =
        admit_pinned_event_record(&batch_frontier, &record, std::slice::from_ref(&event))?;
    let batch = transition.batch_manifest.record(
        &frontier.authority_root,
        receipt.clone(),
        &result.authority_root,
    )?;
    batch.verify_entry(&MachineCommandArchiveEntry {
        admission: admission.clone(),
        command: ArchivedCommandRecord::from_private(&record),
        events: vec![event.clone()],
    })?;
    let mut delta = build_machine_root_delta(
        frontier,
        &result,
        vec![event],
        admission,
        &record,
        &inputs.command_index_proof,
    );
    add_material_to_root_delta(&mut delta, material);
    delta.batch_admission_order.push(batch.batch_id.clone());
    delta.batches.insert(batch.batch_id.clone(), batch);
    Ok((delta, result))
}

/// Prepare the single atomic publication after every transition source page
/// reached its exact end.
#[doc(hidden)]
pub fn prepare_pinned_transition_final(
    frontier: &MachineAuthorityFrontier,
    transition: &MachinePagedTransitionCurrent,
    inputs: MachinePagedFinalizeInputs,
) -> Result<PreparedPinnedPagedFinalize> {
    frontier.verify()?;
    transition.verify()?;
    validate_paged_finalize_inputs(frontier, transition, &inputs)?;

    let mut result_current = inputs.live_run.clone();
    result_current.children = transition.shadow.children.clone();
    result_current.order = transition.shadow.order.clone();
    result_current.indexes = transition.shadow.indexes.clone();
    result_current.reducer_state = MachineRunReducerState::Ready;
    result_current.world_settlement = result_current.indexes.settlement();
    let mut reduction = PinnedRunReduction::default();
    let payload = match &transition.action {
        MachinePagedTransitionAction::CommitScope { .. }
        | MachinePagedTransitionAction::AbortScope { .. } => finalize_paged_scope_action(
            transition,
            &inputs.scopes,
            &mut reduction,
            &mut result_current,
        )?,
        MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun => {
            finalize_paged_run_action(transition, &inputs, &mut reduction, &mut result_current)?
        }
    };
    let event = build_paged_final_event(transition, &inputs.live_run.last_event, payload)?;
    result_current.last_event.clone_from(&event.event_id);
    let receipt = CommandReceipt {
        command_id: transition.command_id.clone(),
        status: CommandReceiptStatus::Applied,
        event_ids: vec![event.event_id.clone()],
        error_code: None,
        message: None,
        observed_precondition: transition.envelope.expected_precondition.clone(),
        current_precondition: Some(result_current.precondition_token()),
    };
    let (machine_delta, result_frontier) =
        admit_paged_final_batch(frontier, transition, &inputs, &receipt, event)?;
    let plans = paged_shadow_root_plans(transition, &reduction)?;
    let mut prepared = PreparedPinnedPagedFinalize {
        frontier: result_frontier,
        transition: transition.clone(),
        live_run: inputs.live_run,
        result_current,
        receipt,
        machine_delta,
        reduction,
        plans,
        local_authority: String::new(),
    };
    prepared.refresh_local_authority()?;
    Ok(prepared)
}

/// Resolve only global idempotency authority. Replay and pending results return
/// before the caller is allowed to resolve a Run or any semantic child leaf.
#[doc(hidden)]
pub fn prepare_pinned_command(
    frontier: &MachineAuthorityFrontier,
    command_proof: &MachinePinnedCommandProof,
    envelope: CommandEnvelope,
) -> Result<PinnedMachineCommandPreparation> {
    prepare_pinned_command_inner(frontier, command_proof, envelope, None)
}

fn prepare_pinned_command_inner(
    frontier: &MachineAuthorityFrontier,
    command_proof: &MachinePinnedCommandProof,
    envelope: CommandEnvelope,
    batch_context: Option<BatchReadContext>,
) -> Result<PinnedMachineCommandPreparation> {
    frontier.verify()?;
    validate_envelope(&envelope)?;
    match command_proof.verify(frontier, &envelope)? {
        MachinePinnedCommandLookup::Replay(receipt) => {
            return Ok(PinnedMachineCommandPreparation::Replay(Box::new(
                PinnedMachineCommandReplay {
                    receipt,
                    frontier: frontier.clone(),
                },
            )));
        }
        MachinePinnedCommandLookup::Pending(transition) => {
            return Ok(PinnedMachineCommandPreparation::Pending(transition));
        }
        MachinePinnedCommandLookup::Vacant => {}
    }
    let index_proof = command_proof
        .vacant_proof()
        .ok_or_else(|| {
            CoreError::IdentityMismatch("vacant command lookup lost its proof".to_owned())
        })?
        .clone();
    let mut prepared = PreparedPinnedRunLookup {
        frontier: frontier.clone(),
        envelope,
        index_proof,
        batch_context,
        local_authority: String::new(),
    };
    prepared.local_authority = canonical_digest(&(
        PREPARED_PINNED_RUN_LOOKUP_AUTHORITY_DOMAIN,
        &prepared.frontier,
        &prepared.envelope,
        &prepared.index_proof,
        &prepared.batch_context,
    ))?;
    Ok(PinnedMachineCommandPreparation::Lookup(Box::new(prepared)))
}

/// Typed membership changes for one persistent reducer index.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineRunIndexMembershipDelta {
    /// Exact selected index.
    pub selector: MachineRunIndexSelector,
    /// New identities inserted by this transition.
    pub inserted: BTreeSet<String>,
    /// Existing identities removed by this transition.
    pub removed: BTreeSet<String>,
}

/// Exact append to one proposal-order persistent log.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineRunLogAppendDelta {
    /// Exact selected log.
    pub selector: MachineRunLogSelector,
    /// Values appended in semantic order. Plan and binding lineages may repeat.
    pub values: Vec<String>,
}

/// Exact bounded Run-current and typed child-leaf change.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineRunDelta {
    /// Target Run.
    pub run_id: String,
    /// Digest of the exact parent Run current, null when starting a Run.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_current_digest: Option<String>,
    /// Final scalar Run current with Store-computed physical roots.
    pub result_current: MachineRunCurrent,
    /// Scope leaves inserted or replaced by exact key.
    pub scopes: BTreeMap<String, MachineScopeCurrent>,
    /// Effect leaves inserted or replaced by exact key.
    pub effects: BTreeMap<String, crate::EffectProjection>,
    /// Obligation leaves inserted or replaced by exact key.
    pub obligations: BTreeMap<String, ObligationProjection>,
    /// Attempt leaves inserted or replaced by exact key.
    pub attempts: BTreeMap<String, crate::AttemptProjection>,
    /// Reducer-index membership mutations.
    pub indexes: Vec<MachineRunIndexMembershipDelta>,
    /// Proposal-order persistent-log appends.
    pub logs: Vec<MachineRunLogAppendDelta>,
}

/// Complete typed physical transition produced by the pinned reducer seam.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedMachineRootDelta {
    /// Existing closed Machine semantic-root delta.
    pub machine: MachineRootDelta,
    /// Target Run change; every applied Event has exactly one.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub run: Option<MachineRunDelta>,
    /// Global fact leaves inserted by this Event.
    pub facts: BTreeMap<String, String>,
}

/// Exact no-write response for an already admitted command.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedMachineCommandReplay {
    /// Stable original receipt.
    pub receipt: CommandReceipt,
    /// Unchanged pinned frontier observed by this replay.
    pub frontier: MachineAuthorityFrontier,
}

/// Result of exact command lookup and local semantic preparation.
pub enum PinnedMachineCommandPreparation {
    /// Exact lost-ack replay; no Store write is authorized.
    Replay(Box<PinnedMachineCommandReplay>),
    /// Exact crash-resume handle for an already reserved paged command.
    Pending(Box<MachinePagedTransitionCurrent>),
    /// New command requiring only the exact Run-current lookup next.
    Lookup(Box<PreparedPinnedRunLookup>),
}

/// Exact Run-current/absence lookup performed only after command authority
/// proved this is not a replay or an existing reservation.
#[derive(Debug, Clone, PartialEq)]
pub struct MachinePinnedRunLookup {
    machine_revision: String,
    run_id: String,
    runs_root: MachineMapRoot,
    run: Option<MachineRunCurrent>,
}

impl MachinePinnedRunLookup {
    /// Assemble a resolver-owned Run-current lookup.
    #[doc(hidden)]
    pub fn new(
        machine_revision: String,
        run_id: String,
        runs_root: MachineMapRoot,
        run: Option<MachineRunCurrent>,
    ) -> Self {
        Self {
            machine_revision,
            run_id,
            runs_root,
            run,
        }
    }
}

/// Fresh command after global lookup but before Run-current resolution.
pub struct PreparedPinnedRunLookup {
    frontier: MachineAuthorityFrontier,
    envelope: CommandEnvelope,
    index_proof: MachineCommandIndexProof,
    batch_context: Option<BatchReadContext>,
    local_authority: String,
}

/// Result after stale-action resolution.
pub enum PinnedMachineRunPreparation {
    /// Closed conflict admission; no semantic child reads are required.
    Conflict(Box<PreparedPinnedMachineTransition>),
    /// Current command needs its closed semantic read set.
    Reads(Box<PreparedPinnedReadCommand>),
}

/// Fresh non-stale command ready for command-shaped exact reads.
pub struct PreparedPinnedReadCommand {
    frontier: MachineAuthorityFrontier,
    envelope: CommandEnvelope,
    index_proof: MachineCommandIndexProof,
    lookup: MachinePinnedRunLookup,
    batch_context: Option<BatchReadContext>,
    local_authority: String,
}

/// Fully reduced fresh command after the exact semantic reads are validated.
pub enum PinnedMachineFreshPreparation {
    /// Ordinary one-Event or closed Conflict transition.
    Prepared(Box<PreparedPinnedMachineTransition>),
    /// New K-page command reservation and Run fence.
    PagedBegin(Box<PreparedPinnedPagedBegin>),
}

impl PreparedPinnedRunLookup {
    /// Resolve only the target Run current. A stale command closes as Conflict
    /// here without loading Plans, Artifacts, Events, admissions, or child maps.
    ///
    /// # Errors
    ///
    /// Returns an error when local preparation, revision, Run-current proof, or
    /// stale-action authority is invalid.
    pub fn resolve_run(
        self,
        lookup: MachinePinnedRunLookup,
    ) -> Result<PinnedMachineRunPreparation> {
        let expected = canonical_digest(&(
            PREPARED_PINNED_RUN_LOOKUP_AUTHORITY_DOMAIN,
            &self.frontier,
            &self.envelope,
            &self.index_proof,
            &self.batch_context,
        ))?;
        if expected != self.local_authority {
            return Err(CoreError::IdentityMismatch(
                "prepared pinned Run lookup lost local authority".to_owned(),
            ));
        }
        crate::validate_content_id("Machine pinned revision", &lookup.machine_revision)?;
        validate_identity("Machine Run lookup", &lookup.run_id)?;
        lookup.runs_root.verify()?;
        if lookup.run_id != self.envelope.run_id || lookup.runs_root != self.frontier.runs {
            return Err(CoreError::IdentityMismatch(
                "Machine Run lookup does not match the pinned frontier".to_owned(),
            ));
        }
        if let Some(run) = &lookup.run {
            run.verify()?;
            if run.run_id != lookup.run_id {
                return Err(CoreError::IdentityMismatch(
                    "Machine Run lookup changed the target identity".to_owned(),
                ));
            }
        }
        let current_precondition = lookup
            .run
            .as_ref()
            .map(MachineRunCurrent::precondition_token);
        if !matches!(self.envelope.command, Command::StartRun { .. }) {
            let observed = self.envelope.expected_precondition.clone().ok_or_else(|| {
                CoreError::Validation("mutating commands require expected_precondition".to_owned())
            })?;
            if Some(observed.as_str()) != current_precondition.as_deref() {
                return Ok(PinnedMachineRunPreparation::Conflict(Box::new(
                    prepare_conflict(
                        &self.frontier,
                        &self.index_proof,
                        self.envelope,
                        current_precondition,
                    )?,
                )));
            }
            if lookup.run.is_none() {
                return Err(CoreError::NotFound(format!(
                    "Run {} does not exist",
                    lookup.run_id
                )));
            }
        } else if lookup.run.is_some() {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} already exists",
                lookup.run_id
            )));
        }
        let mut prepared = PreparedPinnedReadCommand {
            frontier: self.frontier,
            envelope: self.envelope,
            index_proof: self.index_proof,
            lookup,
            batch_context: self.batch_context,
            local_authority: String::new(),
        };
        prepared.local_authority = canonical_digest(&(
            PREPARED_PINNED_READ_COMMAND_AUTHORITY_DOMAIN,
            &prepared.frontier,
            &prepared.envelope,
            &prepared.index_proof,
            &prepared.lookup.machine_revision,
            &prepared.lookup.run_id,
            &prepared.lookup.runs_root,
            &prepared.lookup.run,
            &prepared.batch_context,
        ))?;
        Ok(PinnedMachineRunPreparation::Reads(Box::new(prepared)))
    }
}

impl PreparedPinnedReadCommand {
    /// Request the complete bounded Scope witness for this exact batch member.
    ///
    /// # Errors
    ///
    /// Returns an error for a changed Scope, open child, or a multi-command
    /// closure that requires the paged protocol.
    pub fn inline_scope_read_requirement(
        &self,
        scope: &MachineScopeCurrent,
    ) -> Result<Option<MachineInlineScopeReadRequirement>> {
        inline_scope::scope_read_requirement(self.batch_context.as_ref(), &self.envelope, scope)
    }

    /// Validate the complete command-shaped semantic view and reduce it.
    ///
    /// # Errors
    ///
    /// Returns an error when the read set differs from the pinned lookup,
    /// contains unrelated/missing authority, or semantic reduction fails.
    pub fn prepare(self, inputs: MachineRunReadInputs) -> Result<PinnedMachineFreshPreparation> {
        let expected = canonical_digest(&(
            PREPARED_PINNED_READ_COMMAND_AUTHORITY_DOMAIN,
            &self.frontier,
            &self.envelope,
            &self.index_proof,
            &self.lookup.machine_revision,
            &self.lookup.run_id,
            &self.lookup.runs_root,
            &self.lookup.run,
            &self.batch_context,
        ))?;
        if expected != self.local_authority {
            return Err(CoreError::IdentityMismatch(
                "prepared pinned read command lost local authority".to_owned(),
            ));
        }
        if inputs.machine_revision != self.lookup.machine_revision
            || inputs.run_id != self.lookup.run_id
            || inputs.runs_root != self.lookup.runs_root
            || inputs.run != self.lookup.run
        {
            return Err(CoreError::IdentityMismatch(
                "Machine semantic reads changed the pinned Run lookup".to_owned(),
            ));
        }
        let inline = match &self.envelope.command {
            Command::CommitScope { scope_id } | Command::AbortScope { scope_id } => inputs
                .scopes
                .get(scope_id)
                .and_then(Option::as_ref)
                .map(|scope| self.inline_scope_read_requirement(scope))
                .transpose()?
                .flatten(),
            _ => None,
        };
        let reads =
            MachineRunReadSet::prepare_with_inline(&self.frontier, &self.envelope, inputs, inline)?;
        if matches!(
            self.envelope.command,
            Command::CommitScope { .. }
                | Command::AbortScope { .. }
                | Command::FailRun { .. }
                | Command::CancelRun { .. }
        ) && reads.inline_scope.is_none()
        {
            return Ok(PinnedMachineFreshPreparation::PagedBegin(Box::new(
                prepare_paged_begin(&self.frontier, &reads, self.envelope)?,
            )));
        }
        Ok(PinnedMachineFreshPreparation::Prepared(Box::new(
            prepare_applied(&self.frontier, &self.index_proof, &reads, self.envelope)?,
        )))
    }
}

/// Typed result of atomically reserving one paged command and fencing its Run.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedMachinePagedBegin {
    /// Frontier after the resolver-owned top-level maps were updated.
    pub frontier: MachineAuthorityFrontier,
    /// Pre-fence Run-current digest used as the stable transition source.
    pub source_run_current_digest: String,
    /// Fenced live Run leaf.
    pub fenced_run: MachineRunCurrent,
    /// Complete persisted transition leaf.
    pub transition: MachinePagedTransitionCurrent,
}

/// Locally prepared global reservation and Run fence.
pub struct PreparedPinnedPagedBegin {
    frontier: MachineAuthorityFrontier,
    source_run_current_digest: String,
    fenced_run: MachineRunCurrent,
    transition: MachinePagedTransitionCurrent,
    plans: Vec<MachinePreparedRootMutation>,
    local_authority: String,
}

/// Proposal material staging that must complete before the paged reservation.
/// This local value is not serializable and cannot be restored as authority.
pub struct PreparedPinnedPagedMaterial {
    begin: PreparedPinnedPagedBegin,
    manifest: MachinePagedBatchManifest,
    plans: Vec<MachinePreparedRootMutation>,
    local_authority: String,
}

impl PreparedPinnedPagedMaterial {
    /// Exact private-map writes for the complete proposed material.
    ///
    /// # Errors
    ///
    /// Returns an error if the frozen local staging authority changed.
    pub fn root_mutations(&self) -> Result<&[MachinePreparedRootMutation]> {
        self.verify_local_authority()?;
        Ok(&self.plans)
    }

    /// Bind staging roots and construct the one original batch reservation.
    ///
    /// # Errors
    ///
    /// Returns an error unless the updates exactly match every requested
    /// private map and the resulting persisted manifest closes.
    pub fn finish(
        mut self,
        updates: Vec<MachineRunRootUpdate>,
    ) -> Result<PreparedPinnedPagedBegin> {
        self.verify_local_authority()?;
        let mut material = MachinePagedMaterialRoots::empty();
        for (target, root) in consume_bound_root_updates(&self.plans, updates)? {
            match target {
                MachineRunRootUpdateTarget::PagedMaterialPlans => {
                    material.plans = require_map_root(&target, &root)?.clone();
                }
                MachineRunRootUpdateTarget::PagedMaterialArtifacts => {
                    material.artifacts = require_map_root(&target, &root)?.clone();
                }
                _ => {
                    return Err(CoreError::IdentityMismatch(
                        "paged material stage returned another root".to_owned(),
                    ));
                }
            }
        }
        self.begin.transition.batch_manifest = self.manifest;
        self.begin.transition.staged_material = material;
        self.begin.transition.transition_id = self.begin.transition.expected_transition_id()?;
        self.begin.transition.verify()?;
        self.begin.fenced_run.reducer_state = MachineRunReducerState::Transitioning {
            transition_id: self.begin.transition.transition_id.clone(),
        };
        self.begin.plans = paged_begin_root_plans(
            &self.begin.frontier,
            &self.begin.fenced_run,
            &self.begin.fenced_run,
            &self.begin.transition,
        )?;
        self.begin.refresh_local_authority()?;
        Ok(self.begin)
    }

    fn verify_local_authority(&self) -> Result<()> {
        self.begin.verify_local_authority()?;
        if self.local_authority
            != canonical_digest(&(&self.begin.local_authority, &self.manifest, &self.plans))?
        {
            return Err(CoreError::IdentityMismatch(
                "paged material staging lost local authority".to_owned(),
            ));
        }
        Ok(())
    }
}

impl PreparedPinnedPagedBegin {
    fn refresh_local_authority(&mut self) -> Result<()> {
        self.local_authority = canonical_digest(&(
            PREPARED_PAGED_BEGIN_AUTHORITY_DOMAIN,
            &self.frontier,
            &self.source_run_current_digest,
            &self.fenced_run,
            &self.transition,
            &self.plans,
        ))?;
        Ok(())
    }

    /// Exact same-CAS top-level mutations: Run fence, command reservation, and
    /// transition leaf. None may be applied independently.
    ///
    /// # Errors
    ///
    /// Returns an error if the locally prepared reservation/fence authority was
    /// altered.
    pub fn root_mutations(&self) -> Result<&[MachinePreparedRootMutation]> {
        self.verify_local_authority()?;
        Ok(&self.plans)
    }

    /// Bind the three Store-computed roots and publish the in-progress state.
    ///
    /// # Errors
    ///
    /// Returns an error unless all three root results exactly match the single
    /// prepared atomic reservation/fence mutation.
    pub fn finish(mut self, updates: Vec<MachineRunRootUpdate>) -> Result<PinnedMachinePagedBegin> {
        self.verify_local_authority()?;
        let supplied = consume_bound_root_updates(&self.plans, updates)?;
        for (target, result) in supplied {
            match target {
                MachineRunRootUpdateTarget::Runs => {
                    self.frontier.runs = require_map_root(&target, &result)?.clone();
                }
                MachineRunRootUpdateTarget::PendingCommands => {
                    self.frontier.pending_commands = require_map_root(&target, &result)?.clone();
                }
                MachineRunRootUpdateTarget::PagedTransitions => {
                    self.frontier.paged_transitions = require_map_root(&target, &result)?.clone();
                }
                _ => unreachable!("paged begin accepted a non-global target"),
            }
        }
        self.frontier.verify()?;
        Ok(PinnedMachinePagedBegin {
            frontier: self.frontier,
            source_run_current_digest: self.source_run_current_digest,
            fenced_run: self.fenced_run,
            transition: self.transition,
        })
    }

    fn verify_local_authority(&self) -> Result<()> {
        let expected = canonical_digest(&(
            PREPARED_PAGED_BEGIN_AUTHORITY_DOMAIN,
            &self.frontier,
            &self.source_run_current_digest,
            &self.fenced_run,
            &self.transition,
            &self.plans,
        ))?;
        if expected != self.local_authority {
            return Err(CoreError::IdentityMismatch(
                "prepared paged Machine begin lost local authority".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Final transition after the Store map layer returned the exact changed roots.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedMachineTransition {
    /// Closed command receipt.
    pub receipt: CommandReceipt,
    /// Exact semantic and physical result frontier.
    pub frontier: MachineAuthorityFrontier,
    /// Complete typed root delta.
    pub delta: PinnedMachineRootDelta,
}

/// How one batch command obtains its exact Run precondition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "source", content = "value", rename_all = "snake_case")]
pub enum MachinePinnedBatchPrecondition {
    /// Exact parent-StateRoot precondition for the first command on a Run.
    Parent(Option<String>),
    /// Exact result precondition produced by the prior batch command on this Run.
    Derived,
}

/// One immutable command intent in an ordered pinned batch manifest.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct MachinePinnedBatchCommand {
    /// Caller/framework idempotency identity.
    pub command_id: String,
    /// Authenticated actor identity.
    pub actor: String,
    /// Target Run identity.
    pub run_id: String,
    /// Parent or batch-derived precondition source.
    pub precondition: MachinePinnedBatchPrecondition,
    /// Exact semantic command.
    pub command: Command,
}

impl MachinePinnedBatchCommand {
    fn intent_hash(&self) -> Result<String> {
        command_intent_hash(&CommandEnvelope {
            command_version: crate::COMMAND_VERSION.to_owned(),
            command_id: self.command_id.clone(),
            actor: self.actor.clone(),
            run_id: self.run_id.clone(),
            expected_precondition: None,
            command: self.command.clone(),
        })
    }

    fn envelope(&self, derived: Option<&str>) -> Result<CommandEnvelope> {
        let expected_precondition = match &self.precondition {
            MachinePinnedBatchPrecondition::Parent(value) => value.clone(),
            MachinePinnedBatchPrecondition::Derived => Some(
                derived
                    .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                        family: "Machine batch derived precondition",
                        key: self.run_id.clone(),
                    })?
                    .to_owned(),
            ),
        };
        let envelope = CommandEnvelope {
            command_version: crate::COMMAND_VERSION.to_owned(),
            command_id: self.command_id.clone(),
            actor: self.actor.clone(),
            run_id: self.run_id.clone(),
            expected_precondition,
            command: self.command.clone(),
        };
        validate_envelope(&envelope)?;
        Ok(envelope)
    }
}

/// Frozen ordered batch manifest and staged-frontier driver.
pub struct PreparedPinnedCommandBatch {
    batch_id: String,
    parent_frontier: MachineAuthorityFrontier,
    material_frontier: MachineAuthorityFrontier,
    batch_frontier: MachineAuthorityFrontier,
    current_frontier: MachineAuthorityFrontier,
    material_digest: Option<String>,
    material_source: Option<MachineCommandBatchMaterialSource>,
    plan_ids: Vec<String>,
    artifacts: Vec<ArtifactRef>,
    material_delta: Option<MachineRootDelta>,
    proposed_material: Option<MachineMaterialAdmission>,
    commands: Vec<MachinePinnedBatchCommand>,
    next_index: usize,
    run_preconditions: BTreeMap<String, String>,
    receipts: Vec<CommandReceipt>,
    steps: Vec<PinnedMachineRootDelta>,
}

/// Terminal all-or-none pinned batch receipt and physical closure.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct PinnedMachineBatchTransition {
    /// Complete persistent all-or-none batch authority.
    pub batch: MachineCommandBatchRecord,
    /// Unique final semantic/physical frontier.
    pub frontier: MachineAuthorityFrontier,
    /// Aggregate semantic Machine root delta.
    pub machine: MachineRootDelta,
    /// Ordered typed physical closures applied only inside the owning CAS overlay.
    pub steps: Vec<PinnedMachineRootDelta>,
}

/// Freeze one ordered atomic batch before any overlay root is applied.
///
/// # Errors
///
/// Returns an error when the batch is empty/oversized, repeats a command,
/// carries an invalid first/derived precondition shape, mixes a paged command,
/// or does not bind its optional material admission.
#[doc(hidden)]
fn verify_pinned_batch_commands(commands: &[MachinePinnedBatchCommand]) -> Result<()> {
    if commands.is_empty() || commands.len() > MAX_PINNED_COMMAND_BATCH_COMMANDS {
        return Err(CoreError::Validation(
            "pinned command batch is empty or exceeds its closed bound".to_owned(),
        ));
    }
    if commands.len() > 1
        && commands.iter().any(|entry| {
            matches!(
                entry.command,
                Command::FailRun { .. } | Command::CancelRun { .. }
            )
        })
    {
        return Err(CoreError::Validation(
            "paged Machine command must be the sole command in a batch".to_owned(),
        ));
    }
    let mut command_ids = BTreeSet::new();
    let mut seen_runs = BTreeSet::new();
    for entry in commands {
        if !command_ids.insert(entry.command_id.clone()) {
            return Err(CoreError::Validation(
                "pinned command batch repeats a command identity".to_owned(),
            ));
        }
        let first_for_run = seen_runs.insert(entry.run_id.clone());
        if first_for_run
            != matches!(
                entry.precondition,
                MachinePinnedBatchPrecondition::Parent(_)
            )
        {
            return Err(CoreError::Validation(
                "pinned batch precondition source does not match Run order".to_owned(),
            ));
        }
        let envelope = entry.envelope(
            matches!(entry.precondition, MachinePinnedBatchPrecondition::Derived)
                .then_some("batch-derived"),
        )?;
        validate_identity("batch command ID", &envelope.command_id)?;
        validate_identity("batch actor", &envelope.actor)?;
        validate_identity("batch Run", &envelope.run_id)?;
        if matches!(entry.command, Command::StartRun { .. })
            && !matches!(
                entry.precondition,
                MachinePinnedBatchPrecondition::Parent(None)
            )
        {
            return Err(CoreError::Validation(
                "StartRun batch command must be the first absent-Run command".to_owned(),
            ));
        }
    }
    Ok(())
}

fn verify_pinned_batch_material_source(
    commands: &[MachinePinnedBatchCommand],
    material: Option<&MachineMaterialAdmission>,
) -> Result<()> {
    if let Some(material) = material
        && let Some(command) = commands
            .iter()
            .find(|entry| entry.command_id == material.source_command_id)
        && let Command::StartRun {
            material_digest, ..
        } = &command.command
        && material_digest != &material.material_digest
    {
        return Err(CoreError::IdentityMismatch(
            "StartRun batch manifest changed its material digest".to_owned(),
        ));
    }
    Ok(())
}

fn pinned_batch_members(
    commands: &[MachinePinnedBatchCommand],
) -> Result<Vec<MachineCommandBatchMember>> {
    commands
        .iter()
        .enumerate()
        .map(|(position, command)| {
            Ok(MachineCommandBatchMember {
                position: u32::try_from(position)
                    .map_err(|error| CoreError::Validation(error.to_string()))?,
                command_id: command.command_id.clone(),
                intent_hash: command.intent_hash()?,
                semantic_hash: String::new(),
            })
        })
        .collect()
}

fn pinned_batch_material_membership(
    commands: &[MachinePinnedBatchCommand],
    material: Option<&MachineMaterialAdmission>,
) -> Result<(Vec<String>, Vec<ArtifactRef>)> {
    let mut plan_ids = Vec::new();
    let mut artifacts = Vec::new();
    for command in commands {
        let (command_plans, command_artifacts) = command_material_membership(&command.command)?;
        plan_ids.extend(command_plans);
        artifacts.extend(command_artifacts);
    }
    if let Some(material) = material {
        plan_ids.extend(material.plans.iter().map(|plan| plan.plan_id.clone()));
        artifacts.extend(
            material
                .artifacts
                .iter()
                .map(|artifact| artifact.reference.clone()),
        );
    }
    plan_ids.sort();
    plan_ids.dedup();
    artifacts.sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
    artifacts.dedup_by(|left, right| left.artifact_id == right.artifact_id);
    Ok((plan_ids, artifacts))
}

/// Freeze one ordered atomic batch before any overlay root is applied.
///
/// # Errors
///
/// Returns an error for invalid command order, preconditions, material source,
/// bounds, or immutable membership. Paged batches defer semantic admission.
#[doc(hidden)]
pub fn prepare_pinned_command_batch(
    frontier: &MachineAuthorityFrontier,
    commands: Vec<MachinePinnedBatchCommand>,
    material: Option<(MachineMaterialAdmission, MachineMaterialParentReads)>,
) -> Result<PreparedPinnedCommandBatch> {
    frontier.verify()?;
    verify_pinned_batch_commands(&commands)?;
    let proposed_material = material.as_ref().map(|(material, _)| material.clone());
    verify_pinned_batch_material_source(&commands, proposed_material.as_ref())?;
    let material_digest = proposed_material
        .as_ref()
        .map(|value| value.material_digest.clone());
    let material_source = proposed_material
        .as_ref()
        .map(MachineMaterialAdmission::source_manifest);
    let members = pinned_batch_members(&commands)?;
    let (plan_ids, artifacts) =
        pinned_batch_material_membership(&commands, proposed_material.as_ref())?;
    let batch_id = machine_command_batch_id(
        &frontier.authority_root,
        &members,
        material_digest.as_deref(),
        material_source.as_ref(),
        &plan_ids,
        &artifacts,
    )?;
    let prepared_material = material
        .map(|(material, reads)| prepare_material_delta(frontier, &material, &reads))
        .transpose()?;
    let material_delta = prepared_material
        .as_ref()
        .map(|prepared| prepared.delta.clone());
    let material_frontier =
        prepared_material.map_or_else(|| frontier.clone(), |prepared| prepared.frontier);
    let mut batch_frontier = material_frontier.clone();
    batch_frontier.batch_admission_commitment = append_material_commitment(
        MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN,
        &batch_frontier.batch_admission_commitment,
        &batch_id,
    )?;
    batch_frontier.batch_count = batch_frontier
        .batch_count
        .checked_add(1)
        .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::Validation("Machine batch count overflowed".to_owned()))?;
    batch_frontier.authority_root = batch_frontier.expected_authority_root()?;
    batch_frontier.verify()?;
    Ok(PreparedPinnedCommandBatch {
        batch_id,
        parent_frontier: frontier.clone(),
        material_frontier,
        batch_frontier: batch_frontier.clone(),
        current_frontier: if is_paged_batch(&commands) {
            frontier.clone()
        } else {
            batch_frontier
        },
        material_digest,
        material_source,
        plan_ids,
        artifacts,
        material_delta,
        proposed_material,
        commands,
        next_index: 0,
        run_preconditions: BTreeMap::new(),
        receipts: Vec::new(),
        steps: Vec::new(),
    })
}

fn is_paged_batch(commands: &[MachinePinnedBatchCommand]) -> bool {
    matches!(commands, [command] if matches!(command.command,
        Command::CommitScope { .. } | Command::AbortScope { .. }
            | Command::FailRun { .. } | Command::CancelRun { .. }))
}

impl PreparedPinnedCommandBatch {
    /// Prepare the exact next member while retaining Core-owned batch context.
    ///
    /// # Errors
    ///
    /// Returns an error for an exhausted batch or invalid global command proof.
    pub fn prepare_next(
        &mut self,
        proof: &MachinePinnedCommandProof,
    ) -> Result<PinnedMachineCommandPreparation> {
        if is_paged_batch(&self.commands)
            && matches!(
                self.next_command().map(|command| &command.command),
                Some(Command::CommitScope { .. } | Command::AbortScope { .. })
            )
        {
            self.current_frontier.clone_from(&self.batch_frontier);
        }
        let context = BatchReadContext {
            batch_id: self.batch_id.clone(),
            position: u32::try_from(self.next_index)
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            length: u32::try_from(self.commands.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?,
        };
        prepare_pinned_command_inner(
            &self.current_frontier,
            proof,
            self.next_envelope()?,
            Some(context),
        )
    }

    /// Frozen batch content identity.
    pub fn batch_id(&self) -> &str {
        &self.batch_id
    }

    /// Exact material-only frontier before batch admission.
    pub fn material_frontier(&self) -> &MachineAuthorityFrontier {
        &self.material_frontier
    }

    /// Optional exact material-only delta applied first in the same overlay.
    pub fn material_delta(&self) -> Option<&MachineRootDelta> {
        if is_paged_batch(&self.commands) {
            None
        } else {
            self.material_delta.as_ref()
        }
    }

    /// Complete local proposal for command-shaped reads before any admission.
    pub fn proposed_material(&self) -> Option<&MachineMaterialAdmission> {
        self.proposed_material.as_ref()
    }

    /// Consume the sole paged command into its original persisted manifest.
    ///
    /// # Errors
    ///
    /// Returns an error unless the fresh begin is the exact unstarted sole
    /// command at the frozen parent. No semantic material or batch is admitted.
    pub fn into_paged_begin(
        self,
        mut begin: PreparedPinnedPagedBegin,
    ) -> Result<PreparedPinnedPagedMaterial> {
        begin.verify_local_authority()?;
        if !is_paged_batch(&self.commands)
            || self.next_index != 0
            || !self.steps.is_empty()
            || begin.frontier != self.current_frontier
            || begin.transition.envelope != self.next_envelope()?
        {
            return Err(CoreError::IdentityMismatch(
                "paged begin is not the exact unstarted batch".to_owned(),
            ));
        }
        begin.frontier.clone_from(&self.parent_frontier);
        begin.refresh_local_authority()?;
        let envelope = &begin.transition.envelope;
        let manifest = MachinePagedBatchManifest {
            batch_version: MACHINE_COMMAND_BATCH_VERSION.to_owned(),
            batch_id: self.batch_id,
            parent_authority_root: self.parent_frontier.authority_root,
            member: MachineCommandBatchMember {
                position: 0,
                command_id: envelope.command_id.clone(),
                intent_hash: command_intent_hash(envelope)?,
                semantic_hash: canonical_digest(envelope)?,
            },
            material_digest: self.material_digest,
            material_source: self.material_source,
            plan_ids: self.plan_ids,
            artifacts: self.artifacts,
        };
        manifest.verify(envelope)?;
        let mut plans = Vec::new();
        if let Some(material) = self.proposed_material {
            plans.push(prepared_root_mutation(
                MachineRunRootUpdateTarget::PagedMaterialPlans,
                MachinePhysicalRoot::Map(MachineMapRoot::empty()),
                u64::try_from(material.plans.len())
                    .map_err(|error| CoreError::Validation(error.to_string()))?,
                MachineTypedRootMutation::PutMaterialPlans(
                    material
                        .plans
                        .into_iter()
                        .map(|plan| (plan.plan_id.clone(), plan))
                        .collect(),
                ),
            )?);
            plans.push(prepared_root_mutation(
                MachineRunRootUpdateTarget::PagedMaterialArtifacts,
                MachinePhysicalRoot::Map(MachineMapRoot::empty()),
                u64::try_from(material.artifacts.len())
                    .map_err(|error| CoreError::Validation(error.to_string()))?,
                MachineTypedRootMutation::PutMaterialArtifacts(
                    material
                        .artifacts
                        .into_iter()
                        .map(|artifact| (artifact.reference.artifact_id.clone(), artifact))
                        .collect(),
                ),
            )?);
        }
        let local_authority = canonical_digest(&(&begin.local_authority, &manifest, &plans))?;
        Ok(PreparedPinnedPagedMaterial {
            begin,
            manifest,
            plans,
            local_authority,
        })
    }

    /// Exact staged frontier for preparing the next command step.
    pub fn current_frontier(&self) -> &MachineAuthorityFrontier {
        &self.current_frontier
    }

    /// Next immutable command intent, if any.
    pub fn next_command(&self) -> Option<&MachinePinnedBatchCommand> {
        self.commands.get(self.next_index)
    }

    /// Build the exact next envelope using the prior accepted staged Run root.
    ///
    /// # Errors
    ///
    /// Returns an error when the batch is complete or a derived Run
    /// precondition is not yet available.
    pub fn next_envelope(&self) -> Result<CommandEnvelope> {
        let next = self.next_command().ok_or_else(|| {
            CoreError::IllegalTransition("pinned command batch is complete".to_owned())
        })?;
        next.envelope(self.run_preconditions.get(&next.run_id).map(String::as_str))
    }

    /// Accept one fully finished typed root DAG from the same Store overlay.
    ///
    /// # Errors
    ///
    /// Returns an error unless the transition is the exact next command,
    /// extends the current staged frontier, is applied, and publishes one Run
    /// current under its exact result frontier.
    pub fn accept_step(mut self, mut transition: PinnedMachineTransition) -> Result<Self> {
        let expected = self.next_envelope()?;
        rebind_transition_to_batch(
            &mut transition,
            &self.batch_id,
            u32::try_from(self.next_index)
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            u32::try_from(self.commands.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?,
        )?;
        transition.frontier.verify()?;
        let machine = &transition.delta.machine;
        if machine.parent_authority_root != self.current_frontier.authority_root
            || machine.result_authority_root != transition.frontier.authority_root
            || machine.commands.len() != 1
            || machine.admissions.len() != 1
        {
            return Err(CoreError::IdentityMismatch(
                "pinned batch step does not extend its exact staged frontier".to_owned(),
            ));
        }
        let record = machine.commands.get(&expected.command_id).ok_or_else(|| {
            CoreError::IdentityMismatch("pinned batch step changed command order".to_owned())
        })?;
        if record.envelope != expected
            || transition.receipt != record.receipt
            || transition.receipt.status != CommandReceiptStatus::Applied
            || machine.admissions[0].command_id != expected.command_id
        {
            return Err(CoreError::IdentityMismatch(
                "pinned batch step changed its exact envelope or receipt".to_owned(),
            ));
        }
        let run = transition.delta.run.as_ref().ok_or_else(|| {
            CoreError::IdentityMismatch("applied batch step has no Run delta".to_owned())
        })?;
        if run.run_id != expected.run_id {
            return Err(CoreError::IdentityMismatch(
                "pinned batch step returned another Run".to_owned(),
            ));
        }
        self.run_preconditions
            .insert(run.run_id.clone(), run.result_current.precondition_token());
        self.current_frontier = transition.frontier;
        self.receipts.push(transition.receipt);
        self.steps.push(transition.delta);
        self.next_index += 1;
        Ok(self)
    }

    /// Finish the complete all-or-none batch closure.
    ///
    /// # Errors
    ///
    /// Returns an error when a step is missing or aggregate root-delta closure
    /// is discontinuous or conflicting.
    pub fn finish(self) -> Result<PinnedMachineBatchTransition> {
        if self.next_index != self.commands.len() {
            return Err(CoreError::IllegalTransition(
                "pinned command batch is incomplete".to_owned(),
            ));
        }
        let mut machine = aggregate_batch_machine_delta(
            &self.parent_frontier,
            &self.batch_frontier,
            &self.current_frontier,
            self.material_delta.as_ref(),
            &self.steps,
        )?;
        let members = self
            .commands
            .iter()
            .zip(&self.steps)
            .enumerate()
            .map(|(position, (command, step))| {
                let record = step
                    .machine
                    .commands
                    .get(&command.command_id)
                    .ok_or_else(|| {
                        CoreError::NotFound(format!(
                            "batch command {} has no record",
                            command.command_id
                        ))
                    })?;
                Ok(MachineCommandBatchMember {
                    position: u32::try_from(position)
                        .map_err(|error| CoreError::Validation(error.to_string()))?,
                    command_id: command.command_id.clone(),
                    intent_hash: command.intent_hash()?,
                    semantic_hash: record.semantic_hash.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let event_ids = self
            .receipts
            .iter()
            .flat_map(|receipt| receipt.event_ids.iter().cloned())
            .collect::<Vec<_>>();
        let mut batch = MachineCommandBatchRecord {
            batch_version: MACHINE_COMMAND_BATCH_VERSION.to_owned(),
            batch_id: self.batch_id,
            parent_authority_root: self.parent_frontier.authority_root.clone(),
            admission_parent_authority_root: self.parent_frontier.authority_root.clone(),
            members,
            material_digest: self.material_digest,
            material_source: self.material_source,
            plan_ids: self.plan_ids,
            artifacts: self.artifacts,
            receipts: self.receipts,
            event_ids,
            result_authority_root: self.current_frontier.authority_root.clone(),
            batch_receipt_id: String::new(),
        };
        batch.batch_receipt_id = batch.expected_receipt_id()?;
        batch.verify()?;
        machine
            .batches
            .insert(batch.batch_id.clone(), batch.clone());
        machine.batch_admission_order.push(batch.batch_id.clone());
        Ok(PinnedMachineBatchTransition {
            batch,
            frontier: self.current_frontier,
            machine,
            steps: self.steps,
        })
    }
}

fn rebind_transition_to_batch(
    transition: &mut PinnedMachineTransition,
    batch_id: &str,
    position: u32,
    batch_len: u32,
) -> Result<()> {
    let machine = &mut transition.delta.machine;
    if machine.commands.len() != 1 || machine.admissions.len() != 1 {
        return Err(CoreError::IdentityMismatch(
            "pinned batch step has no unique command authority".to_owned(),
        ));
    }
    let record =
        machine.commands.values_mut().next().ok_or_else(|| {
            CoreError::NotFound("pinned batch step command is missing".to_owned())
        })?;
    batch_id.clone_into(&mut record.batch_id);
    record.batch_position = position;
    record.batch_len = batch_len;
    record.verify()?;
    let admission = machine
        .admissions
        .first_mut()
        .ok_or_else(|| CoreError::NotFound("pinned batch step admission is missing".to_owned()))?;
    batch_id.clone_into(&mut admission.batch_id);
    admission.batch_position = position;
    admission.batch_len = batch_len;
    admission.command_record_digest = canonical_digest(record)?;
    admission.admission_id = admission.expected_id()?;
    verify_admission_record(admission, &record.to_private())?;
    transition.frontier.admission_head = Some(admission.admission_id.clone());
    transition.frontier.authority_root = transition.frontier.expected_authority_root()?;
    transition.frontier.verify()?;
    machine.result_authority_root = transition.frontier.authority_root.clone();
    Ok(())
}

fn aggregate_batch_machine_delta(
    parent: &MachineAuthorityFrontier,
    batch_frontier: &MachineAuthorityFrontier,
    result: &MachineAuthorityFrontier,
    material: Option<&MachineRootDelta>,
    steps: &[PinnedMachineRootDelta],
) -> Result<MachineRootDelta> {
    let mut aggregate = MachineRootDelta {
        root_delta_version: MachineRootDelta::VERSION.to_owned(),
        delta_version: MachineDelta::VERSION.to_owned(),
        parent_authority_root: parent.authority_root.clone(),
        result_authority_root: result.authority_root.clone(),
        parent_anchor_id: parent.base_anchor_id.clone(),
        result_anchor_id: result.base_anchor_id.clone(),
        plans: BTreeMap::new(),
        plan_admission_order: Vec::new(),
        artifacts: BTreeMap::new(),
        artifact_admission_order: Vec::new(),
        batches: BTreeMap::new(),
        batch_admission_order: Vec::new(),
        removed_event_ids: Vec::new(),
        removed_admission_ids: Vec::new(),
        removed_command_ids: BTreeSet::new(),
        removed_batch_ids: BTreeSet::new(),
        removed_command_index_proof_ids: BTreeSet::new(),
        base: None,
        base_anchor: None,
        archive_segment: None,
        events: Vec::new(),
        admissions: Vec::new(),
        commands: BTreeMap::new(),
        command_index_proofs: BTreeMap::new(),
    };
    if let Some(material) = material {
        if material.parent_authority_root != parent.authority_root
            || !material.removed_event_ids.is_empty()
            || !material.removed_admission_ids.is_empty()
            || !material.removed_command_ids.is_empty()
            || !material.removed_batch_ids.is_empty()
            || !material.removed_command_index_proof_ids.is_empty()
            || material.base.is_some()
            || material.base_anchor.is_some()
            || material.archive_segment.is_some()
            || !material.events.is_empty()
            || !material.admissions.is_empty()
            || !material.commands.is_empty()
            || !material.command_index_proofs.is_empty()
        {
            return Err(CoreError::IdentityMismatch(
                "pinned batch material prefix is not one exact material-only transition".to_owned(),
            ));
        }
        merge_unique_batch_values(&mut aggregate.plans, &material.plans, "Plan")?;
        merge_unique_batch_values(&mut aggregate.artifacts, &material.artifacts, "Artifact")?;
        aggregate
            .plan_admission_order
            .extend(material.plan_admission_order.iter().cloned());
        aggregate
            .artifact_admission_order
            .extend(material.artifact_admission_order.iter().cloned());
    }
    let mut expected_parent = batch_frontier.authority_root.as_str();
    for step in steps {
        let delta = &step.machine;
        if delta.parent_authority_root != expected_parent
            || !delta.removed_event_ids.is_empty()
            || !delta.removed_admission_ids.is_empty()
            || !delta.removed_command_ids.is_empty()
            || !delta.removed_command_index_proof_ids.is_empty()
            || delta.base.is_some()
            || delta.base_anchor.is_some()
            || delta.archive_segment.is_some()
        {
            return Err(CoreError::IdentityMismatch(
                "pinned batch contains a discontinuous or compacting step".to_owned(),
            ));
        }
        merge_unique_batch_values(&mut aggregate.plans, &delta.plans, "Plan")?;
        merge_unique_batch_values(&mut aggregate.artifacts, &delta.artifacts, "Artifact")?;
        merge_unique_batch_values(&mut aggregate.commands, &delta.commands, "command")?;
        merge_unique_batch_values(
            &mut aggregate.command_index_proofs,
            &delta.command_index_proofs,
            "command proof",
        )?;
        aggregate
            .plan_admission_order
            .extend(delta.plan_admission_order.iter().cloned());
        aggregate
            .artifact_admission_order
            .extend(delta.artifact_admission_order.iter().cloned());
        aggregate.events.extend(delta.events.iter().cloned());
        aggregate
            .admissions
            .extend(delta.admissions.iter().cloned());
        expected_parent = &delta.result_authority_root;
    }
    if expected_parent != result.authority_root {
        return Err(CoreError::IdentityMismatch(
            "pinned batch aggregate does not reach its final frontier".to_owned(),
        ));
    }
    Ok(aggregate)
}

fn merge_unique_batch_values<T: Clone + PartialEq>(
    target: &mut BTreeMap<String, T>,
    values: &BTreeMap<String, T>,
    kind: &str,
) -> Result<()> {
    for (id, value) in values {
        if target.insert(id.clone(), value.clone()).is_some() {
            return Err(CoreError::IdentityMismatch(format!(
                "pinned batch repeats {kind} identity {id}"
            )));
        }
    }
    Ok(())
}

/// Verify an all-or-none hot or archived replay of one persistent batch.
///
/// # Errors
///
/// Returns an error when the request order/intent/material differs, any member
/// record is missing or belongs to another batch position, a derived
/// precondition or semantic hash differs, or any receipt is not exact.
#[doc(hidden)]
pub fn verify_pinned_command_batch_replay(
    batch: &MachineCommandBatchRecord,
    requested: &[MachinePinnedBatchCommand],
    records: &[ArchivedCommandRecord],
    material_digest: Option<&str>,
) -> Result<Vec<CommandReceipt>> {
    batch.verify()?;
    if requested.len() != batch.members.len()
        || records.len() != batch.members.len()
        || material_digest != batch.material_digest.as_deref()
    {
        return Err(CoreError::IdentityMismatch(
            "pinned batch replay is partial or has different material".to_owned(),
        ));
    }
    let mut run_preconditions = BTreeMap::<String, String>::new();
    for (index, ((request, member), record)) in requested
        .iter()
        .zip(&batch.members)
        .zip(records)
        .enumerate()
    {
        let first_for_run = !run_preconditions.contains_key(&request.run_id);
        if first_for_run
            != matches!(
                request.precondition,
                MachinePinnedBatchPrecondition::Parent(_)
            )
        {
            return Err(CoreError::IdentityMismatch(
                "pinned batch replay changed a Run precondition source".to_owned(),
            ));
        }
        let envelope =
            request.envelope(run_preconditions.get(&request.run_id).map(String::as_str))?;
        if usize::try_from(member.position).ok() != Some(index)
            || member.command_id != request.command_id
            || member.intent_hash != request.intent_hash()?
            || member.semantic_hash != canonical_digest(&envelope)?
            || record.envelope != envelope
            || record.semantic_hash != member.semantic_hash
            || record.receipt != batch.receipts[index]
            || record.batch_id != batch.batch_id
            || usize::try_from(record.batch_position).ok() != Some(index)
            || usize::try_from(record.batch_len).ok() != Some(requested.len())
        {
            return Err(CoreError::IdentityMismatch(
                "pinned batch replay member changed order or authority".to_owned(),
            ));
        }
        let current = record.receipt.current_precondition.clone().ok_or_else(|| {
            CoreError::IdentityMismatch(
                "applied batch replay member has no result precondition".to_owned(),
            )
        })?;
        run_preconditions.insert(request.run_id.clone(), current);
    }
    Ok(batch.receipts.clone())
}

/// Locally derived transition awaiting the exact Store-computed roots.
///
/// This type is deliberately non-serializable. Its private expected-root set
/// and local digest prevent a caller from turning arbitrary postconditions into
/// persistent semantic authority.
pub struct PreparedPinnedMachineTransition {
    receipt: CommandReceipt,
    frontier: MachineAuthorityFrontier,
    machine_delta: MachineRootDelta,
    parent_current_digest: Option<String>,
    result_current: Option<MachineRunCurrent>,
    scopes: BTreeMap<String, MachineScopeCurrent>,
    effects: BTreeMap<String, crate::EffectProjection>,
    obligations: BTreeMap<String, ObligationProjection>,
    attempts: BTreeMap<String, crate::AttemptProjection>,
    indexes: Vec<MachineRunIndexMembershipDelta>,
    logs: Vec<MachineRunLogAppendDelta>,
    facts: BTreeMap<String, String>,
    expected_roots: BTreeMap<MachineRunRootUpdateTarget, u64>,
    parent_roots: BTreeMap<MachineRunRootUpdateTarget, MachinePhysicalRoot>,
    local_authority: String,
}

#[derive(serde::Serialize)]
struct PreparedPinnedAuthority<'a> {
    receipt: &'a CommandReceipt,
    frontier: &'a MachineAuthorityFrontier,
    machine_delta: &'a MachineRootDelta,
    parent_current_digest: &'a Option<String>,
    result_current: &'a Option<MachineRunCurrent>,
    scopes: &'a BTreeMap<String, MachineScopeCurrent>,
    effects: &'a BTreeMap<String, crate::EffectProjection>,
    obligations: &'a BTreeMap<String, ObligationProjection>,
    attempts: &'a BTreeMap<String, crate::AttemptProjection>,
    indexes: &'a [MachineRunIndexMembershipDelta],
    logs: &'a [MachineRunLogAppendDelta],
    facts: &'a BTreeMap<String, String>,
    expected_roots: Vec<(&'a MachineRunRootUpdateTarget, &'a u64)>,
    parent_roots: Vec<(&'a MachineRunRootUpdateTarget, &'a MachinePhysicalRoot)>,
}

impl PreparedPinnedMachineTransition {
    /// Borrow the exact Core-derived Event payloads for typed Store witnesses.
    ///
    /// # Errors
    ///
    /// Returns an error if the local prepared transition authority changed.
    /// These Events do not grant independent append or mutation authority.
    pub fn events(&self) -> Result<&[Event]> {
        self.verify_local_authority()?;
        Ok(&self.machine_delta.events)
    }

    /// Root applies for per-Scope indexes and proposal logs. These must finish
    /// before the changed Scope leaves can enter the Run Scope map.
    ///
    /// # Errors
    ///
    /// Returns an error if local authority was altered or an exact Scope-root
    /// mutation cannot be derived.
    pub fn scope_root_mutations(&self) -> Result<Vec<MachinePreparedRootMutation>> {
        self.verify_local_authority()?;
        self.mutation_plans(is_scope_root_target)
    }

    /// Bind per-Scope root results and advance to the Run-child stage.
    ///
    /// # Errors
    ///
    /// Returns an error unless every supplied Scope root exactly matches every
    /// prepared request and all changed Scope leaves remain valid.
    pub fn finish_scope_roots(
        mut self,
        updates: Vec<MachineRunRootUpdate>,
    ) -> Result<PreparedPinnedRunTransition> {
        self.verify_local_authority()?;
        let supplied = self.consume_stage_updates(updates, is_scope_root_target)?;
        let result = self.result_current.as_mut().ok_or_else(|| {
            CoreError::IdentityMismatch(
                "conflict admission attempted to update a Scope root".to_owned(),
            )
        });
        if !supplied.is_empty() {
            let result = result?;
            for (target, root) in supplied {
                match &target {
                    MachineRunRootUpdateTarget::Index(selector) => apply_result_index_root(
                        result,
                        &mut self.scopes,
                        selector,
                        require_map_root(&target, &root)?.clone(),
                    )?,
                    MachineRunRootUpdateTarget::Log(selector) => apply_result_log_root(
                        result,
                        &mut self.scopes,
                        selector,
                        require_log_root(&target, &root)?.clone(),
                    )?,
                    _ => unreachable!("scope stage predicate admitted a non-Scope target"),
                }
            }
            for scope in self.scopes.values() {
                scope.verify()?;
            }
        }
        finalize_prepared_authority(&mut self)?;
        Ok(PreparedPinnedRunTransition { inner: self })
    }

    fn authority_preimage(&self) -> PreparedPinnedAuthority<'_> {
        PreparedPinnedAuthority {
            receipt: &self.receipt,
            frontier: &self.frontier,
            machine_delta: &self.machine_delta,
            parent_current_digest: &self.parent_current_digest,
            result_current: &self.result_current,
            scopes: &self.scopes,
            effects: &self.effects,
            obligations: &self.obligations,
            attempts: &self.attempts,
            indexes: &self.indexes,
            logs: &self.logs,
            facts: &self.facts,
            expected_roots: self.expected_roots.iter().collect(),
            parent_roots: self.parent_roots.iter().collect(),
        }
    }

    fn verify_local_authority(&self) -> Result<()> {
        if canonical_digest(&self.authority_preimage())? != self.local_authority {
            return Err(CoreError::IdentityMismatch(
                "prepared pinned Machine transition lost local authority".to_owned(),
            ));
        }
        Ok(())
    }

    fn mutation_plans(
        &self,
        select: impl Fn(&MachineRunRootUpdateTarget) -> bool,
    ) -> Result<Vec<MachinePreparedRootMutation>> {
        self.expected_roots
            .iter()
            .filter(|(target, _)| select(target))
            .map(|(target, expected_count)| {
                let parent = self.parent_roots.get(target).ok_or_else(|| {
                    CoreError::Validation(format!(
                        "pinned Machine root {target:?} has no exact parent"
                    ))
                })?;
                prepared_root_mutation(
                    target.clone(),
                    parent.clone(),
                    *expected_count,
                    self.typed_mutation(target)?,
                )
            })
            .collect()
    }

    fn typed_mutation(
        &self,
        target: &MachineRunRootUpdateTarget,
    ) -> Result<MachineTypedRootMutation> {
        match target {
            MachineRunRootUpdateTarget::Scopes => {
                Ok(MachineTypedRootMutation::PutScopes(self.scopes.clone()))
            }
            MachineRunRootUpdateTarget::Effects => {
                Ok(MachineTypedRootMutation::PutEffects(self.effects.clone()))
            }
            MachineRunRootUpdateTarget::Obligations => Ok(
                MachineTypedRootMutation::PutObligations(self.obligations.clone()),
            ),
            MachineRunRootUpdateTarget::Attempts => {
                Ok(MachineTypedRootMutation::PutAttempts(self.attempts.clone()))
            }
            MachineRunRootUpdateTarget::Index(selector) => {
                Ok(MachineTypedRootMutation::UpdateMembership(
                    self.indexes
                        .iter()
                        .filter(|delta| &delta.selector == selector)
                        .cloned()
                        .collect(),
                ))
            }
            MachineRunRootUpdateTarget::Log(selector) => Ok(MachineTypedRootMutation::AppendLog(
                self.logs
                    .iter()
                    .filter(|delta| &delta.selector == selector)
                    .cloned()
                    .collect(),
            )),
            MachineRunRootUpdateTarget::Runs => {
                let result = self.result_current.clone().ok_or_else(|| {
                    CoreError::Validation(
                        "conflict admission unexpectedly requested a Run root".to_owned(),
                    )
                })?;
                Ok(MachineTypedRootMutation::PutRuns(BTreeMap::from([(
                    result.run_id.clone(),
                    result,
                )])))
            }
            MachineRunRootUpdateTarget::Facts => {
                Ok(MachineTypedRootMutation::PutFacts(self.facts.clone()))
            }
            MachineRunRootUpdateTarget::PendingCommands
            | MachineRunRootUpdateTarget::PagedTransitions
            | MachineRunRootUpdateTarget::PagedMaterialPlans
            | MachineRunRootUpdateTarget::PagedMaterialArtifacts => Err(CoreError::Validation(
                "ordinary pinned transition referenced a paged root".to_owned(),
            )),
        }
    }

    fn consume_stage_updates(
        &self,
        updates: Vec<MachineRunRootUpdate>,
        select: impl Fn(&MachineRunRootUpdateTarget) -> bool,
    ) -> Result<BTreeMap<MachineRunRootUpdateTarget, MachinePhysicalRoot>> {
        let plans = self
            .mutation_plans(select)?
            .into_iter()
            .map(|plan| (plan.target.clone(), plan))
            .collect::<BTreeMap<_, _>>();
        let mut supplied = BTreeMap::new();
        for update in updates {
            update.result.verify()?;
            let plan = plans.get(&update.target).ok_or_else(|| {
                CoreError::IdentityMismatch(format!(
                    "pinned Machine stage did not request root {:?}",
                    update.target
                ))
            })?;
            if update.parent != plan.parent
                || update.mutation_digest != plan.mutation_digest
                || update.result.count() != plan.expected_count
            {
                return Err(CoreError::IdentityMismatch(format!(
                    "pinned Machine root {:?} is not the requested parent, mutation, and result count",
                    update.target
                )));
            }
            if supplied.insert(update.target, update.result).is_some() {
                return Err(CoreError::Validation(
                    "pinned Machine stage repeated a physical root update".to_owned(),
                ));
            }
        }
        if supplied.keys().ne(plans.keys()) {
            return Err(CoreError::IdentityMismatch(
                "pinned Machine stage result roots do not match its exact requested set".to_owned(),
            ));
        }
        Ok(supplied)
    }
}

/// Prepared transition after every nested Scope root is final.
pub struct PreparedPinnedRunTransition {
    inner: PreparedPinnedMachineTransition,
}

impl PreparedPinnedRunTransition {
    /// Root applies for Run child maps, Run indexes, and Run proposal logs.
    ///
    /// # Errors
    ///
    /// Returns an error if local authority was altered or an exact Run-root
    /// mutation cannot be derived.
    pub fn run_root_mutations(&self) -> Result<Vec<MachinePreparedRootMutation>> {
        self.inner.verify_local_authority()?;
        self.inner.mutation_plans(is_run_root_target)
    }

    /// Bind Run-child results and advance to the global map stage.
    ///
    /// # Errors
    ///
    /// Returns an error unless every supplied Run child/index/log root exactly
    /// matches the prepared transition and the result Run verifies.
    pub fn finish_run_roots(
        mut self,
        updates: Vec<MachineRunRootUpdate>,
    ) -> Result<PreparedPinnedGlobalTransition> {
        self.inner.verify_local_authority()?;
        let supplied = self
            .inner
            .consume_stage_updates(updates, is_run_root_target)?;
        if !supplied.is_empty() {
            let result = self.inner.result_current.as_mut().ok_or_else(|| {
                CoreError::IdentityMismatch(
                    "conflict admission attempted to update a Run child root".to_owned(),
                )
            })?;
            for (target, root) in supplied {
                match &target {
                    MachineRunRootUpdateTarget::Scopes => {
                        result.children.scopes = require_map_root(&target, &root)?.clone();
                    }
                    MachineRunRootUpdateTarget::Effects => {
                        result.children.effects = require_map_root(&target, &root)?.clone();
                    }
                    MachineRunRootUpdateTarget::Obligations => {
                        result.children.obligations = require_map_root(&target, &root)?.clone();
                    }
                    MachineRunRootUpdateTarget::Attempts => {
                        result.children.attempts = require_map_root(&target, &root)?.clone();
                    }
                    MachineRunRootUpdateTarget::Index(selector) => apply_result_index_root(
                        result,
                        &mut self.inner.scopes,
                        selector,
                        require_map_root(&target, &root)?.clone(),
                    )?,
                    MachineRunRootUpdateTarget::Log(selector) => apply_result_log_root(
                        result,
                        &mut self.inner.scopes,
                        selector,
                        require_log_root(&target, &root)?.clone(),
                    )?,
                    _ => unreachable!("Run stage predicate admitted a global target"),
                }
            }
        }
        if let Some(result) = &self.inner.result_current {
            result.verify()?;
        }
        finalize_prepared_authority(&mut self.inner)?;
        Ok(PreparedPinnedGlobalTransition { inner: self.inner })
    }
}

/// Prepared transition whose final Run-current leaf is fully determined.
pub struct PreparedPinnedGlobalTransition {
    inner: PreparedPinnedMachineTransition,
}

impl PreparedPinnedGlobalTransition {
    /// Root applies for the global Run and fact maps.
    ///
    /// # Errors
    ///
    /// Returns an error if local authority was altered or an exact global
    /// mutation cannot be derived.
    pub fn global_root_mutations(&self) -> Result<Vec<MachinePreparedRootMutation>> {
        self.inner.verify_local_authority()?;
        self.inner.mutation_plans(is_global_root_target)
    }

    /// Bind the global map results and produce the final typed transition.
    ///
    /// # Errors
    ///
    /// Returns an error unless every global result exactly matches the prepared
    /// requests and the final semantic frontier verifies.
    pub fn finish(mut self, updates: Vec<MachineRunRootUpdate>) -> Result<PinnedMachineTransition> {
        self.inner.verify_local_authority()?;
        let supplied = self
            .inner
            .consume_stage_updates(updates, is_global_root_target)?;
        for (target, root) in supplied {
            match target {
                MachineRunRootUpdateTarget::Runs => {
                    self.inner.frontier.runs = require_map_root(&target, &root)?.clone();
                }
                MachineRunRootUpdateTarget::Facts => {
                    self.inner.frontier.facts = require_map_root(&target, &root)?.clone();
                }
                _ => unreachable!("global stage predicate admitted a non-global target"),
            }
        }
        self.inner.frontier.verify()?;
        let run = self
            .inner
            .result_current
            .map(|result_current| MachineRunDelta {
                run_id: result_current.run_id.clone(),
                parent_current_digest: self.inner.parent_current_digest,
                result_current,
                scopes: self.inner.scopes,
                effects: self.inner.effects,
                obligations: self.inner.obligations,
                attempts: self.inner.attempts,
                indexes: self.inner.indexes,
                logs: self.inner.logs,
            });
        Ok(PinnedMachineTransition {
            receipt: self.inner.receipt,
            frontier: self.inner.frontier,
            delta: PinnedMachineRootDelta {
                machine: self.inner.machine_delta,
                run,
                facts: self.inner.facts,
            },
        })
    }
}

fn is_scope_root_target(target: &MachineRunRootUpdateTarget) -> bool {
    matches!(
        target,
        MachineRunRootUpdateTarget::Index(
            MachineRunIndexSelector::ScopeEffects { .. }
                | MachineRunIndexSelector::ScopeMutatingEffects { .. }
                | MachineRunIndexSelector::ScopeAbortTransitions { .. }
                | MachineRunIndexSelector::ScopeAbortBlockers { .. }
        ) | MachineRunRootUpdateTarget::Log(
            MachineRunLogSelector::ScopeEffects { .. }
                | MachineRunLogSelector::ScopeMutatingEffects { .. }
        )
    )
}

fn is_run_root_target(target: &MachineRunRootUpdateTarget) -> bool {
    matches!(
        target,
        MachineRunRootUpdateTarget::Scopes
            | MachineRunRootUpdateTarget::Effects
            | MachineRunRootUpdateTarget::Obligations
            | MachineRunRootUpdateTarget::Attempts
            | MachineRunRootUpdateTarget::Index(_)
            | MachineRunRootUpdateTarget::Log(_)
    ) && !is_scope_root_target(target)
}

fn is_global_root_target(target: &MachineRunRootUpdateTarget) -> bool {
    matches!(
        target,
        MachineRunRootUpdateTarget::Runs | MachineRunRootUpdateTarget::Facts
    )
}

fn require_map_root<'a>(
    target: &MachineRunRootUpdateTarget,
    root: &'a MachinePhysicalRoot,
) -> Result<&'a MachineMapRoot> {
    match root {
        MachinePhysicalRoot::Map(root) => Ok(root),
        MachinePhysicalRoot::Log(_) => Err(CoreError::IdentityMismatch(format!(
            "pinned Machine root {target:?} requires a map result"
        ))),
    }
}

fn consume_bound_root_updates(
    plans: &[MachinePreparedRootMutation],
    updates: Vec<MachineRunRootUpdate>,
) -> Result<BTreeMap<MachineRunRootUpdateTarget, MachinePhysicalRoot>> {
    let plans = plans
        .iter()
        .map(|plan| (plan.target.clone(), plan))
        .collect::<BTreeMap<_, _>>();
    let mut supplied = BTreeMap::new();
    for update in updates {
        update.result.verify()?;
        let plan = plans.get(&update.target).ok_or_else(|| {
            CoreError::IdentityMismatch(format!(
                "pinned Machine apply did not request root {:?}",
                update.target
            ))
        })?;
        if update.parent != plan.parent
            || update.mutation_digest != plan.mutation_digest
            || update.result.count() != plan.expected_count
        {
            return Err(CoreError::IdentityMismatch(format!(
                "pinned Machine root {:?} is not its requested apply result",
                update.target
            )));
        }
        if supplied.insert(update.target, update.result).is_some() {
            return Err(CoreError::Validation(
                "pinned Machine apply repeated one physical root update".to_owned(),
            ));
        }
    }
    if supplied.keys().ne(plans.keys()) {
        return Err(CoreError::IdentityMismatch(
            "pinned Machine apply result roots do not match its requested set".to_owned(),
        ));
    }
    Ok(supplied)
}

fn require_log_root<'a>(
    target: &MachineRunRootUpdateTarget,
    root: &'a MachinePhysicalRoot,
) -> Result<&'a MachineLogRoot> {
    match root {
        MachinePhysicalRoot::Log(root) => Ok(root),
        MachinePhysicalRoot::Map(_) => Err(CoreError::IdentityMismatch(format!(
            "pinned Machine root {target:?} requires a log result"
        ))),
    }
}

fn apply_result_index_root(
    run: &mut MachineRunCurrent,
    scopes: &mut BTreeMap<String, MachineScopeCurrent>,
    selector: &MachineRunIndexSelector,
    root: MachineMapRoot,
) -> Result<()> {
    match selector {
        MachineRunIndexSelector::GovernanceEffects => run.indexes.governance_effects = root,
        MachineRunIndexSelector::UnknownEffects => run.indexes.unknown_effects = root,
        MachineRunIndexSelector::PendingEffects => run.indexes.pending_effects = root,
        MachineRunIndexSelector::TerminalTransitionEffects => {
            run.indexes.terminal_transition_effects = root;
        }
        MachineRunIndexSelector::OpenScopes => run.indexes.open_scopes = root,
        MachineRunIndexSelector::UnresolvedObligations => {
            run.indexes.unresolved_obligations = root;
        }
        MachineRunIndexSelector::ScopeEffects { scope_id }
        | MachineRunIndexSelector::ScopeMutatingEffects { scope_id }
        | MachineRunIndexSelector::ScopeAbortTransitions { scope_id }
        | MachineRunIndexSelector::ScopeAbortBlockers { scope_id } => {
            let scope =
                scopes
                    .get_mut(scope_id)
                    .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                        family: "result Machine Scope current",
                        key: scope_id.clone(),
                    })?;
            match selector {
                MachineRunIndexSelector::ScopeEffects { .. } => scope.effects = root,
                MachineRunIndexSelector::ScopeMutatingEffects { .. } => {
                    scope.mutating_effects = root;
                }
                MachineRunIndexSelector::ScopeAbortTransitions { .. } => {
                    scope.abort_transitions = root;
                }
                MachineRunIndexSelector::ScopeAbortBlockers { .. } => {
                    scope.abort_blockers = root;
                }
                _ => unreachable!("scope selector was matched above"),
            }
        }
    }
    Ok(())
}

fn apply_result_log_root(
    run: &mut MachineRunCurrent,
    scopes: &mut BTreeMap<String, MachineScopeCurrent>,
    selector: &MachineRunLogSelector,
    root: MachineLogRoot,
) -> Result<()> {
    match selector {
        MachineRunLogSelector::Scopes => run.order.scopes = root,
        MachineRunLogSelector::Effects => run.order.effects = root,
        MachineRunLogSelector::Obligations => run.order.obligations = root,
        MachineRunLogSelector::Attempts => run.order.attempts = root,
        MachineRunLogSelector::Plans => {
            run.plan_lineage = root.clone();
            run.order.plans = root;
        }
        MachineRunLogSelector::Bindings => {
            run.binding_lineage = root.clone();
            run.order.bindings = root;
        }
        MachineRunLogSelector::ScopeEffects { scope_id }
        | MachineRunLogSelector::ScopeMutatingEffects { scope_id } => {
            let scope =
                scopes
                    .get_mut(scope_id)
                    .ok_or_else(|| CoreError::PinnedReadSetIncomplete {
                        family: "result Machine Scope current",
                        key: scope_id.clone(),
                    })?;
            match selector {
                MachineRunLogSelector::ScopeEffects { .. } => scope.effect_order = root,
                MachineRunLogSelector::ScopeMutatingEffects { .. } => {
                    scope.mutating_effect_order = root;
                }
                _ => unreachable!("scope log selector was matched above"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        COMMAND_VERSION, Definition, Expression, IR_VERSION, PlanCandidate, Region, seal_plan,
    };

    struct StartedFixture {
        frontier: MachineAuthorityFrontier,
        current: MachineRunCurrent,
        start: PinnedMachineBatchTransition,
    }

    fn empty_map() -> MachineMapRoot {
        MachineMapRoot::empty()
    }

    fn empty_log() -> MachineLogRoot {
        MachineLogRoot::empty()
    }

    fn revision(label: &str) -> String {
        content_id("cymule.test.pinned-revision/1", &label).expect("test revision derives")
    }

    fn candidate() -> PlanCandidate {
        PlanCandidate {
            ir_version: IR_VERSION.to_owned(),
            name: "pinned_test".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Input,
                },
            }],
            metadata: BTreeMap::new(),
        }
    }

    fn binding(label: &str) -> ArtifactRecord {
        binding_bytes(label.as_bytes().to_vec())
    }

    fn binding_bytes(bytes: Vec<u8>) -> ArtifactRecord {
        ArtifactRecord {
            reference: crate::artifact_ref(crate::EXECUTION_BINDING_ARTIFACT_KIND, &bytes)
                .expect("binding reference derives"),
            bytes,
        }
    }

    fn input() -> ArtifactRecord {
        let bytes = b"{}".to_vec();
        ArtifactRecord {
            reference: crate::artifact_ref(crate::RUN_INPUT_ARTIFACT_KIND, &bytes)
                .expect("input reference derives"),
            bytes,
        }
    }

    fn fake_store_result(plan: &MachinePreparedRootMutation) -> MachineRunRootUpdate {
        let count = plan.expected_count();
        let node = (count != 0)
            .then(|| {
                content_id(
                    "cymule.test.pinned-physical-root/1",
                    &(plan.target(), plan.mutation_digest(), count),
                )
            })
            .transpose()
            .expect("physical result identity derives");
        let result = match plan.parent() {
            MachinePhysicalRoot::Map(_) => MachinePhysicalRoot::Map(MachineMapRoot {
                node,
                entries: count,
            }),
            MachinePhysicalRoot::Log(_) => {
                let ordered_root = node
                    .clone()
                    .unwrap_or_else(|| MachineLogRoot::empty().ordered_root);
                MachinePhysicalRoot::Log(MachineLogRoot {
                    node,
                    len: count,
                    height: u8::from(count != 0),
                    ordered_root,
                })
            }
        };
        plan.bind_result(result)
    }

    fn finish(prepared: PreparedPinnedMachineTransition) -> PinnedMachineTransition {
        let scope_updates = prepared
            .scope_root_mutations()
            .expect("Scope plans derive")
            .iter()
            .map(fake_store_result)
            .collect();
        let run = prepared
            .finish_scope_roots(scope_updates)
            .expect("Scope roots bind");
        let run_updates = run
            .run_root_mutations()
            .expect("Run plans derive")
            .iter()
            .map(fake_store_result)
            .collect();
        let global = run.finish_run_roots(run_updates).expect("Run roots bind");
        let global_updates = global
            .global_root_mutations()
            .expect("global plans derive")
            .iter()
            .map(fake_store_result)
            .collect();
        global.finish(global_updates).expect("global roots bind")
    }

    fn start_fixture_material() -> (MachineStartRunMaterial, CommandEnvelope) {
        let plan = seal_plan(candidate()).expect("test Plan seals");
        let binding = binding("start");
        let input = input();
        let command_id = "command:pinned-start".to_owned();
        let material = MachineStartRunMaterial::new(
            command_id.clone(),
            plan.clone(),
            binding.clone(),
            input.clone(),
        )
        .expect("StartRun material derives");
        let initial_attempt = crate::InitialAttemptSpec {
            attempt_id: content_id("cymule.test.initial-attempt/1", &command_id)
                .expect("Attempt derives"),
            continuation_id: content_id("cymule.test.initial-continuation/1", &command_id)
                .expect("Continuation derives"),
            occurrence_binding: binding.reference.artifact_id.clone(),
            continuation_epoch: 0,
            execution_fence: 1,
        };
        let envelope = CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id,
            actor: "actor:pinned-test".to_owned(),
            run_id: "run:pinned-test".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: binding.reference.artifact_id.clone(),
                input: input.reference.clone(),
                material_digest: material.material_digest().to_owned(),
                initial_attempt,
            },
        };
        (material, envelope)
    }

    fn start_fixture() -> StartedFixture {
        let (material, envelope) = start_fixture_material();
        let frontier =
            MachineAuthorityFrontier::genesis(empty_map(), empty_map(), empty_map(), empty_map())
                .expect("frontier initializes");
        let batch = prepare_pinned_command_batch(
            &frontier,
            vec![MachinePinnedBatchCommand {
                command_id: envelope.command_id.clone(),
                actor: envelope.actor.clone(),
                run_id: envelope.run_id.clone(),
                precondition: MachinePinnedBatchPrecondition::Parent(None),
                command: envelope.command.clone(),
            }],
            None,
        )
        .expect("StartRun batch freezes");
        let frontier = batch.current_frontier().clone();
        let proof = MachinePinnedCommandProof::vacant(
            MachineCommandIndexProof::empty_nonmembership(&envelope.command_id)
                .expect("empty command proof"),
        );
        let PinnedMachineCommandPreparation::Lookup(lookup) =
            prepare_pinned_command(&frontier, &proof, envelope.clone())
                .expect("command lookup prepares")
        else {
            panic!("new command must require a Run lookup");
        };
        let PinnedMachineRunPreparation::Reads(read) = lookup
            .resolve_run(MachinePinnedRunLookup::new(
                revision("start"),
                envelope.run_id.clone(),
                frontier.runs.clone(),
                None,
            ))
            .expect("Run absence resolves")
        else {
            panic!("fresh StartRun must require semantic reads");
        };
        let inputs = MachineRunReadInputs {
            machine_revision: revision("start"),
            run_id: envelope.run_id.clone(),
            runs_root: frontier.runs.clone(),
            facts_root: frontier.facts.clone(),
            run: None,
            new_run_empty_root: Some(empty_map()),
            new_run_empty_log: Some(empty_log()),
            plans: material
                .admission()
                .plans()
                .iter()
                .map(|plan| (plan.plan_id.clone(), None))
                .collect(),
            artifacts: material
                .admission()
                .artifacts()
                .iter()
                .map(|artifact| (artifact.reference.artifact_id.clone(), None))
                .collect(),
            scopes: BTreeMap::new(),
            scope_locations: BTreeMap::new(),
            effects: BTreeMap::new(),
            obligations: BTreeMap::new(),
            attempts: BTreeMap::new(),
            facts: BTreeMap::new(),
            start_material: Some(material),
            index_pages: Vec::new(),
            log_pages: Vec::new(),
        };
        let PinnedMachineFreshPreparation::Prepared(prepared) =
            read.prepare(inputs).expect("StartRun reduces")
        else {
            panic!("StartRun is not paged");
        };
        let transition = finish(*prepared);
        assert_eq!(transition.receipt.status, CommandReceiptStatus::Applied);
        let start = batch
            .accept_step(transition)
            .expect("StartRun batch accepts")
            .finish()
            .expect("StartRun batch closes");
        let current = start.steps[0]
            .run
            .as_ref()
            .expect("StartRun publishes one Run delta")
            .result_current
            .clone();
        StartedFixture {
            frontier: start.frontier.clone(),
            current,
            start,
        }
    }

    fn prepare_fact_reads(
        fixture: &StartedFixture,
        command_id: &str,
        key: &str,
    ) -> (CommandEnvelope, PreparedPinnedReadCommand) {
        let envelope = CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: command_id.to_owned(),
            actor: "actor:pinned-test".to_owned(),
            run_id: fixture.current.run_id.clone(),
            expected_precondition: Some(fixture.current.precondition_token()),
            command: Command::RecordFact {
                key: key.to_owned(),
                value: content_id("cymule.test.fact/1", &key).expect("fact value derives"),
            },
        };
        let proof = MachinePinnedCommandProof::vacant(
            MachineCommandIndexProof::empty_nonmembership(command_id).expect("empty command proof"),
        );
        let PinnedMachineCommandPreparation::Lookup(lookup) =
            prepare_pinned_command(&fixture.frontier, &proof, envelope.clone())
                .expect("fact lookup prepares")
        else {
            panic!("new fact must require Run lookup");
        };
        let PinnedMachineRunPreparation::Reads(read) = lookup
            .resolve_run(MachinePinnedRunLookup::new(
                revision(command_id),
                fixture.current.run_id.clone(),
                fixture.frontier.runs.clone(),
                Some(fixture.current.clone()),
            ))
            .expect("fact Run resolves")
        else {
            panic!("fresh fact must require semantic reads");
        };
        (envelope, *read)
    }

    fn fact_inputs(fixture: &StartedFixture, command_id: &str, key: &str) -> MachineRunReadInputs {
        MachineRunReadInputs {
            machine_revision: revision(command_id),
            run_id: fixture.current.run_id.clone(),
            runs_root: fixture.frontier.runs.clone(),
            facts_root: fixture.frontier.facts.clone(),
            run: Some(fixture.current.clone()),
            new_run_empty_root: None,
            new_run_empty_log: None,
            plans: BTreeMap::new(),
            artifacts: BTreeMap::new(),
            scopes: BTreeMap::new(),
            scope_locations: BTreeMap::new(),
            effects: BTreeMap::new(),
            obligations: BTreeMap::new(),
            attempts: BTreeMap::new(),
            facts: BTreeMap::from([(key.to_owned(), None)]),
            start_material: None,
            index_pages: Vec::new(),
            log_pages: Vec::new(),
        }
    }

    #[test]
    fn pinned_start_and_fact_close_the_staged_root_dag() {
        let fixture = start_fixture();
        assert_eq!(fixture.frontier.runs.entries, 1);
        assert_eq!(fixture.frontier.event_count, 2);
        assert_eq!(fixture.frontier.admission_sequence, 1);
        assert_eq!(fixture.current.children.scopes.entries, 1);
        assert_eq!(fixture.current.order.scopes.len, 1);

        let key = "fact:pinned";
        let (envelope, read) = prepare_fact_reads(&fixture, "command:pinned-fact", key);
        let PinnedMachineFreshPreparation::Prepared(prepared) = read
            .prepare(fact_inputs(&fixture, &envelope.command_id, key))
            .expect("fact reduces")
        else {
            panic!("fact is not paged");
        };
        let scope_plans = prepared
            .scope_root_mutations()
            .expect("Scope stage derives");
        assert!(scope_plans.is_empty());
        let run = prepared
            .finish_scope_roots(Vec::new())
            .expect("empty Scope stage closes");
        assert!(
            run.run_root_mutations()
                .expect("Run stage derives")
                .is_empty()
        );
        let global = run
            .finish_run_roots(Vec::new())
            .expect("empty Run stage closes");
        let plans = global
            .global_root_mutations()
            .expect("global stage derives");
        assert_eq!(plans.len(), 2);
        assert!(plans.iter().any(|plan| {
            matches!(
                plan.typed(),
                MachineTypedRootMutation::PutFacts(values)
                    if values.get(key).is_some_and(|value| value == match &envelope.command {
                        Command::RecordFact { value, .. } => value,
                        _ => unreachable!("fixture is a fact command"),
                    })
            )
        }));
        let transition = global
            .finish(plans.iter().map(fake_store_result).collect())
            .expect("fact global roots bind");
        assert_eq!(transition.frontier.event_count, 3);
        assert_eq!(transition.frontier.facts.entries, 1);
        let last_event = transition
            .delta
            .run
            .expect("fact updates Run")
            .result_current
            .last_event;
        assert_eq!(
            transition.receipt.event_ids.last().map(String::as_str),
            Some(last_event.as_str())
        );
    }

    #[test]
    fn pinned_lookup_and_reads_reject_wrong_root_run_last_event_and_missing_leaf() {
        let fixture = start_fixture();
        let command_id = "command:pinned-negative";
        let key = "fact:pinned-negative";
        let (envelope, read) = prepare_fact_reads(&fixture, command_id, key);

        let mut wrong_last_event = fact_inputs(&fixture, command_id, key);
        wrong_last_event
            .run
            .as_mut()
            .expect("Run exists")
            .last_event = revision("wrong-last-event");
        assert!(matches!(
            read.prepare(wrong_last_event),
            Err(CoreError::IdentityMismatch(message))
                if message.contains("changed the pinned Run lookup")
        ));

        let (_, read) = prepare_fact_reads(&fixture, command_id, key);
        let mut wrong_root = fact_inputs(&fixture, command_id, key);
        wrong_root.runs_root = MachineMapRoot {
            node: Some(revision("wrong-root")),
            entries: 1,
        };
        assert!(matches!(
            read.prepare(wrong_root),
            Err(CoreError::IdentityMismatch(message))
                if message.contains("changed the pinned Run lookup")
        ));

        let (_, read) = prepare_fact_reads(&fixture, command_id, key);
        let mut wrong_run = fact_inputs(&fixture, command_id, key);
        wrong_run.run_id = "run:other".to_owned();
        assert!(matches!(
            read.prepare(wrong_run),
            Err(CoreError::IdentityMismatch(message))
                if message.contains("changed the pinned Run lookup")
        ));

        let (_, read) = prepare_fact_reads(&fixture, command_id, key);
        let mut missing = fact_inputs(&fixture, command_id, key);
        missing.facts.clear();
        assert!(matches!(
            read.prepare(missing),
            Err(CoreError::PinnedReadSetIncomplete { family: "Machine fact", key: missing_key })
                if missing_key == key
        ));

        assert_eq!(
            envelope.expected_precondition.as_deref(),
            Some(fixture.current.precondition_token().as_str())
        );
    }

    #[test]
    fn maximum_artifact_remains_a_valid_pinned_leaf_and_plus_one_is_rejected() {
        let artifact = binding_bytes(vec![u8::MAX; crate::MAX_ARTIFACT_BYTES]);
        let mut total = 0;
        account_read_bytes("Machine Artifact read", &artifact, &mut total)
            .expect("maximum Artifact fits the pinned leaf budget");
        assert!(total <= MAX_PINNED_MACHINE_READ_LEAF_BYTES);
        assert!(
            crate::artifact_ref(
                crate::EXECUTION_BINDING_ARTIFACT_KIND,
                &vec![0; crate::MAX_ARTIFACT_BYTES + 1]
            )
            .is_err()
        );
    }

    #[test]
    fn pinned_missing_run_stale_conflict_never_requests_semantic_reads() {
        let frontier =
            MachineAuthorityFrontier::genesis(empty_map(), empty_map(), empty_map(), empty_map())
                .expect("frontier initializes");
        let envelope = CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:pinned-missing-stale".to_owned(),
            actor: "actor:pinned-test".to_owned(),
            run_id: "run:missing".to_owned(),
            expected_precondition: Some("pre:missing".to_owned()),
            command: Command::RecordFact {
                key: "fact:missing".to_owned(),
                value: content_id("cymule.test.fact/1", &"missing").expect("fact value derives"),
            },
        };
        let proof = MachinePinnedCommandProof::vacant(
            MachineCommandIndexProof::empty_nonmembership(&envelope.command_id)
                .expect("empty command proof"),
        );
        let PinnedMachineCommandPreparation::Lookup(lookup) =
            prepare_pinned_command(&frontier, &proof, envelope.clone())
                .expect("command lookup prepares")
        else {
            panic!("new command requires Run lookup");
        };
        let PinnedMachineRunPreparation::Conflict(conflict) = lookup
            .resolve_run(MachinePinnedRunLookup::new(
                revision("missing-stale"),
                envelope.run_id,
                frontier.runs.clone(),
                None,
            ))
            .expect("missing stale Run resolves as conflict")
        else {
            panic!("missing stale Run must close before semantic reads");
        };
        let transition = finish(*conflict);
        assert_eq!(transition.receipt.status, CommandReceiptStatus::Conflict);
        assert!(transition.receipt.current_precondition.is_none());
        assert!(transition.delta.run.is_none());
        assert_eq!(transition.frontier.admission_sequence, 1);
        assert_eq!(transition.frontier.event_count, 0);
    }

    #[test]
    fn pinned_read_set_counts_empty_pages_and_rejects_command_extras() {
        struct EmptyResolver;

        impl cymule_authenticated_collections::CollectionResolver for EmptyResolver {
            fn load_map_node(
                &mut self,
                _object_id: &str,
            ) -> cymule_authenticated_collections::Result<
                Option<cymule_authenticated_collections::MapNode>,
            > {
                Ok(None)
            }

            fn load_log_node(
                &mut self,
                _object_id: &str,
            ) -> cymule_authenticated_collections::Result<
                Option<cymule_authenticated_collections::LogNode>,
            > {
                Ok(None)
            }
        }

        let fixture = start_fixture();
        let command_id = "command:pinned-empty-pages";
        let key = "fact:empty-pages";
        let (envelope, _) = prepare_fact_reads(&fixture, command_id, key);
        let source = fixture.current.indexes.pending_effects.clone();
        let proof = cymule_authenticated_collections::prove_map_range(
            &source,
            None,
            MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
            cymule_authenticated_collections::MAX_PAGE_BYTES,
            &mut EmptyResolver,
        )
        .expect("empty authenticated proof derives");
        let page = MachineRunIndexPage::verify_proof(
            fixture.current.run_id.clone(),
            MachineRunIndexSelector::PendingEffects,
            &source,
            None,
            &proof,
        )
        .expect("empty authenticated page verifies");
        let mut too_many = fact_inputs(&fixture, command_id, key);
        too_many.index_pages = vec![page.clone(); MAX_PINNED_MACHINE_INDEX_PAGES + 1];
        assert!(matches!(
            MachineRunReadSet::prepare(&fixture.frontier, &envelope, too_many),
            Err(CoreError::Validation(message)) if message.contains("index pages")
        ));

        let mut extra = fact_inputs(&fixture, command_id, key);
        extra.index_pages.push(page);
        let error = MachineRunReadSet::prepare(&fixture.frontier, &envelope, extra)
            .expect_err("command-unrelated empty page must fail");
        assert!(
            matches!(&error, CoreError::Validation(message)
                if message.contains("read shape") || message.contains("read set")),
            "unexpected extra-page error: {error:?}"
        );
    }

    include!("pinned_batch_tests.rs");
    include!("start_material_tests.rs");
}
