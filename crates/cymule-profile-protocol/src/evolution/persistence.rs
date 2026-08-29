use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    ArtifactRecord, ArtifactRef, Definition, SealedPlan, canonical_bytes, content_id,
};
use serde::{Deserialize, Serialize};

use super::{
    EvolutionCommand, EvolutionError, EvolutionResult, GateOutcome, LinkedPlan,
    LiveEvolutionCommand, LiveEvolutionOutcome, LivePublicationReceipt, LiveTemplateUpdate,
    MigrationAdapter, MigrationAdapterRequest, MigrationReceipt, MigrationSafePoint,
    ObservationOutcome, OccurrencePin, PlanEdge, PlanTemplate, ReferenceStrategy, RestartReceipt,
    RolloutDecision, RolloutEvaluation, RolloutGate, RolloutMode, RolloutObservation,
    RolloutTransition, ShadowComparison, ShadowDriver, SubflowReference, SubflowRevision,
    analyze_relink, control::validate_identity,
};

/// Semantic-only M4 persistence command generation.
pub const EVOLUTION_PERSISTENCE_COMMAND_VERSION: &str = "cymule.evolution-persistence-command/4";
/// Exact all-ever M4 receipt generation.
pub const EVOLUTION_PERSISTENCE_RECEIPT_VERSION: &str = "cymule.evolution-persistence-receipt/4";
/// Scalar M4 partition current generation.
pub const EVOLUTION_CURRENT_VERSION: &str = "cymule.evolution-current/2";
/// Normalized M4 leaf generation.
pub const EVOLUTION_STATE_LEAF_VERSION: &str = "cymule.evolution-state-leaf/3";
/// Closed mutation-set identity generation.
pub const EVOLUTION_MUTATION_SET_VERSION: &str = "cymule.evolution-mutation-set/2";
/// Content identity domain for one normalized mutation value.
pub const EVOLUTION_MUTATION_VALUE_VERSION: &str = "cymule.evolution-mutation-value/1";
/// Empty and incremental rollout-evidence root domain.
pub const EVOLUTION_EVIDENCE_ROOT_VERSION: &str = "cymule.evolution-evidence-root/1";
/// Maximum canonical bytes for a semantic command.
pub const MAX_EVOLUTION_COMMAND_BYTES: usize = 4 * 1024 * 1024;
/// Maximum canonical bytes for one normalized state leaf, aligned with the
/// global `StateRoot` value bound.
pub const MAX_EVOLUTION_LEAF_BYTES: usize = 12 * 1024 * 1024;
/// Maximum canonical bytes for one exact command receipt.
pub const MAX_EVOLUTION_RECEIPT_BYTES: usize = 8 * 1024 * 1024;
/// Maximum templates one publication may update atomically.
pub const MAX_EVOLUTION_PUBLICATION_TEMPLATES: usize = 1_024;
/// Maximum normalized leaves one M4 command may read or write.
pub const MAX_EVOLUTION_TRANSITION_LEAVES: usize = 8_192;
/// Maximum canonical accounting for one exact reducer source, including its
/// scalar current, membership keys, and loaded leaves.
pub const MAX_EVOLUTION_SOURCE_BYTES: usize = 64 * 1024 * 1024;
/// Maximum aggregate canonical accounting for one reducer postcondition.
pub const MAX_EVOLUTION_POSTCONDITION_BYTES: usize = 64 * 1024 * 1024;

const EVOLUTION_STATE_KEY_VERSION: &str = "cymule.evolution-state-key/1";
const EVOLUTION_CURRENT_KEY_VERSION: &str = "cymule.evolution-current-key/1";
const EVOLUTION_COMMAND_ALIAS_KEY_VERSION: &str = "cymule.evolution-command-alias-key/1";
const EVOLUTION_RECEIPT_KEY_VERSION: &str = "cymule.evolution-receipt-key/1";
const INITIAL_DECISION_ID_DOMAIN: &str = "cymule.live-initial-decision/1";
const UPDATE_DECISION_ID_DOMAIN: &str = "cymule.live-update-decision/1";
const CANARY_ID_DOMAIN: &str = "cymule.canary/2";
const SHADOW_SUBJECT_ID_DOMAIN: &str = "cymule.shadow-subject/1";
const DEFINITION_CONTRACT_ID_DOMAIN: &str = "cymule.definition-contract/1";
const LINK_RECORD_ID_DOMAIN: &str = "cymule.evolution-link-record/1";
const VIRTUAL_EVOLUTION_SELECTION_ID_DOMAIN: &str = "cymule.virtual-evolution-selection/1";

/// Closed normalized `StateRoot` family. Each family is an independently keyed
/// persistent map and every value is bounded by [`MAX_EVOLUTION_LEAF_BYTES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionStateFamily {
    /// Latest admitted revision for one logical definition reference.
    DefinitionCurrent,
    /// Latest admitted revision for one logical reference and exact contract.
    DefinitionCompatibilityCurrent,
    /// Immutable admitted definition revision.
    DefinitionRecord,
    /// Reverse index from a logical definition reference to affected templates.
    DependencyCurrent,
    /// Latest linked Plan and template specification for one template.
    TemplateCurrent,
    /// Immutable linked-Plan record.
    LinkRecord,
    /// Immutable admitted executable Plan record.
    PlanRecord,
    /// Immutable compatibility edge between Plans.
    EdgeRecord,
    /// Current rollout decision for future selections of one template.
    RolloutCurrent,
    /// Bounded evidence aggregate for one rollout decision.
    RolloutEvidenceCurrent,
    /// Immutable rollout decision record.
    RolloutDecision,
    /// Exact Plan pin for one occurrence.
    OccurrenceCurrent,
    /// Reverse index from deterministic selection to occurrence.
    SelectionCurrent,
    /// Immutable migration receipt record.
    MigrationRecord,
    /// Immutable restart receipt record.
    RestartRecord,
    /// Immutable shadow comparison record.
    ShadowRecord,
    /// Reverse index preventing duplicate shadow subjects.
    ShadowSubjectCurrent,
    /// Immutable rollout observation record.
    ObservationRecord,
    /// Reverse index preventing duplicate occurrence observations.
    ObservationOccurrenceCurrent,
    /// Cross-family owner for one evidence identity.
    EvidenceCurrent,
    /// Completed transition for one source decision.
    DecisionTransitionCurrent,
    /// Immutable rollout transition record.
    TransitionRecord,
}

/// Semantic-only wire command. Provider output and target state are never
/// serializable command fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionPersistenceCommand {
    /// Closed persistence command generation.
    pub persistence_version: String,
    /// Content-derived identity of this semantic command.
    pub persistence_id: String,
    /// M4 authority partition targeted by the command.
    pub evolution_id: String,
    /// Semantic intent and scalar optimistic preconditions.
    pub command: LiveEvolutionCommand,
}

/// Scalar current pointer for one M4 authority partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionCurrent {
    /// Closed scalar-current generation.
    pub current_version: String,
    /// Content-derived identity of this scalar current.
    pub current_id: String,
    /// M4 authority partition owned by this current.
    pub evolution_id: String,
    /// Monotonic semantic revision within the partition.
    pub revision: u64,
    /// Exact receipt that produced this current.
    pub last_receipt_id: String,
}

/// Latest admitted revision for one logical definition reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionDefinitionCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    logical_ref: String,
    /// Highest lineage-local sequence ever allocated, independent of which
    /// immutable historical revision is currently selected as the head.
    max_sequence: u64,
    latest: SubflowRevision,
}

/// Latest admitted revision for one logical reference and exact input/output
/// contract. This preserves `latest_compatible` lookup without scanning
/// immutable definition history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionDefinitionCompatibilityCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    logical_ref: String,
    contract_id: String,
    latest: SubflowRevision,
}

/// Reverse dependency index for one logical definition reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionDependencyCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    logical_ref: String,
    template_ids: BTreeSet<String>,
}

/// Latest template specification and exact linked Plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionTemplateCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template: PlanTemplate,
    linked: LinkedPlan,
}

impl EvolutionTemplateCurrent {
    /// Stable semantic template identity of this exact current record.
    pub fn template_id(&self) -> &str {
        &self.template.template_id
    }

    /// Exact immutable Plan selected by the retained template revision.
    pub fn linked_plan_id(&self) -> &str {
        &self.linked.plan.plan_id
    }
}

/// Immutable link result for one admitted template revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionLinkRecord {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    link_id: String,
    linked: LinkedPlan,
}

/// Immutable admitted executable Plan for one template.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionPlanRecord {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    plan: SealedPlan,
}

/// Immutable compatibility edge between two admitted Plans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionEdgeRecord {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    edge: PlanEdge,
    /// First accepted review or compiler evidence for this structural edge.
    evidence: ArtifactRef,
}

/// Current future-selection decision for one template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionRolloutCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    decision: RolloutDecision,
}

/// Bounded evidence aggregate for one immutable rollout decision. Individual
/// observations and comparisons remain exact keyed records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionRolloutEvidenceCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    decision_id: String,
    target_observations: u64,
    target_failures: u64,
    equivalent_shadows: u64,
    inequivalent_shadows: u64,
    evidence_count: u64,
    evidence_root: String,
}

/// Immutable copy of one rollout decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionRolloutDecisionRecord {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    decision: RolloutDecision,
}

/// Exact immutable Plan pin for one occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionOccurrenceCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    pin: OccurrencePin,
}

/// Exact reverse index preventing one deterministic selection from being
/// assigned to multiple occurrences.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionSelectionCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    selection_id: String,
    occurrence_id: String,
    execution_binding: ArtifactRef,
    decision_id: String,
    plan_id: String,
}

/// Immutable accepted migration receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionMigrationRecord {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    receipt: MigrationReceipt,
}

/// Immutable accepted restart receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionRestartRecord {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    receipt: RestartReceipt,
}

/// Immutable accepted shadow comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionShadowRecord {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    comparison: ShadowComparison,
}

/// Exact reverse index preventing duplicate shadow evidence for one decision
/// and subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionShadowSubjectCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    decision_id: String,
    subject: String,
    comparison_id: String,
}

/// Immutable accepted rollout observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionObservationRecord {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    observation: RolloutObservation,
}

/// Exact reverse index preventing duplicate observations for one occurrence
/// under one rollout decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionObservationOccurrenceCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    decision_id: String,
    occurrence_id: String,
    observation_id: String,
}

/// Closed ownership of a rollout evidence identity across observation and
/// shadow record families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionEvidenceKind {
    /// The identity belongs to an occurrence observation.
    Observation,
    /// The identity belongs to a shadow comparison.
    Shadow,
}

/// Exact cross-family alias for one rollout evidence identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionEvidenceCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    evidence_id: String,
    kind: EvolutionEvidenceKind,
}

/// Exact completed-transition alias for one source decision. Its existence
/// prevents a completed decision from becoming current again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionDecisionTransitionCurrent {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    source_decision_id: String,
    transition_id: String,
}

/// Immutable terminal transition between rollout decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionTransitionRecord {
    leaf_version: String,
    evolution_id: String,
    revision: u64,
    template_id: String,
    transition: RolloutTransition,
}

/// One closed normalized state mutation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvolutionMutation {
    /// Replace the latest definition pointer for one logical reference.
    DefinitionCurrent(Box<EvolutionDefinitionCurrent>),
    /// Replace the latest compatible pointer for one logical reference and contract.
    DefinitionCompatibilityCurrent(Box<EvolutionDefinitionCompatibilityCurrent>),
    /// Insert one immutable definition revision.
    DefinitionRecord(Box<EvolutionDefinitionCurrent>),
    /// Replace one logical-reference dependency index.
    DependencyCurrent(Box<EvolutionDependencyCurrent>),
    /// Replace one template's current link state.
    TemplateCurrent(Box<EvolutionTemplateCurrent>),
    /// Insert one immutable linked-Plan record.
    LinkRecord(Box<EvolutionLinkRecord>),
    /// Insert one immutable executable Plan.
    PlanRecord(Box<EvolutionPlanRecord>),
    /// Insert one immutable Plan compatibility edge.
    EdgeRecord(Box<EvolutionEdgeRecord>),
    /// Replace one template's current rollout decision.
    RolloutCurrent(Box<EvolutionRolloutCurrent>),
    /// Replace one decision's bounded evidence aggregate.
    RolloutEvidenceCurrent(Box<EvolutionRolloutEvidenceCurrent>),
    /// Insert one immutable rollout decision.
    RolloutDecision(Box<EvolutionRolloutDecisionRecord>),
    /// Insert one exact occurrence pin.
    OccurrenceCurrent(Box<EvolutionOccurrenceCurrent>),
    /// Insert one deterministic-selection reverse index.
    SelectionCurrent(Box<EvolutionSelectionCurrent>),
    /// Insert one immutable migration receipt.
    MigrationRecord(Box<EvolutionMigrationRecord>),
    /// Insert one immutable restart receipt.
    RestartRecord(Box<EvolutionRestartRecord>),
    /// Insert one immutable shadow comparison.
    ShadowRecord(Box<EvolutionShadowRecord>),
    /// Insert one shadow-subject reverse index.
    ShadowSubjectCurrent(Box<EvolutionShadowSubjectCurrent>),
    /// Insert one immutable rollout observation.
    ObservationRecord(Box<EvolutionObservationRecord>),
    /// Insert one occurrence-observation reverse index.
    ObservationOccurrenceCurrent(Box<EvolutionObservationOccurrenceCurrent>),
    /// Insert one cross-family evidence owner.
    EvidenceCurrent(Box<EvolutionEvidenceCurrent>),
    /// Insert one completed-decision transition pointer.
    DecisionTransitionCurrent(Box<EvolutionDecisionTransitionCurrent>),
    /// Insert one immutable rollout transition.
    TransitionRecord(Box<EvolutionTransitionRecord>),
}

/// One exact normalized write retained by the semantic receipt. The value
/// digest binds the full typed leaf without duplicating large leaf bytes into
/// every all-ever receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionMutationWrite {
    /// Normalized persistent-map family.
    pub family: EvolutionStateFamily,
    /// Exact content-addressed map key within the family.
    pub storage_key: String,
    /// Content identity of the complete typed mutation value.
    pub value_id: String,
}

/// Exact all-ever command alias checked before any current read or provider I/O.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionCommandAlias {
    /// M4 authority partition that owns the command.
    pub evolution_id: String,
    /// Caller-selected idempotency identity.
    pub command_id: String,
    /// Content-derived semantic command identity.
    pub persistence_id: String,
    /// Exact all-ever receipt selected by this command identity.
    pub receipt_id: String,
}

/// Exact all-ever receipt. It binds the semantic command, parent current,
/// deterministic outcome, and normalized mutation set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionPersistenceReceipt {
    /// Closed receipt generation.
    pub receipt_version: String,
    /// Content-derived identity of the semantic receipt body.
    pub receipt_id: String,
    /// Exact semantic command admitted by the transition.
    pub command: EvolutionPersistenceCommand,
    /// Exact parent scalar current, or null only for genesis.
    #[serde(deserialize_with = "super::model::deserialize_required_nullable")]
    pub parent_current_id: Option<String>,
    /// Durable-derived runtime source witness, or null for commands without M1 state.
    #[serde(deserialize_with = "super::model::deserialize_required_nullable")]
    pub source_witness_id: Option<String>,
    /// Deterministic semantic outcome.
    pub outcome: LiveEvolutionOutcome,
    /// Strictly key-ordered exact normalized writes.
    pub mutations: Vec<EvolutionMutationWrite>,
    /// Content identity of the ordered normalized mutation set.
    pub mutation_id: String,
}

/// Exact scalar-current query, optionally pinned to one physical `StateRoot`
/// revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionCurrentQuery {
    /// M4 authority partition to read.
    pub evolution_id: String,
    /// Exact physical revision constraint, or null to pin the current head once.
    #[serde(deserialize_with = "super::model::deserialize_required_nullable")]
    pub expected_revision: Option<String>,
}

/// Exact command-receipt query, optionally pinned to one physical `StateRoot`
/// revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionReceiptQuery {
    /// M4 authority partition that owns the command.
    pub evolution_id: String,
    /// Caller-selected idempotency identity.
    pub command_id: String,
    /// Exact physical revision constraint, or null to pin the current head once.
    #[serde(deserialize_with = "super::model::deserialize_required_nullable")]
    pub expected_revision: Option<String>,
}

/// Revision-pinned exact scalar-current read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionCurrentRead {
    /// Exact physical `StateRoot` revision observed by this read.
    pub observed_revision: String,
    /// Scalar partition current, or null when the exact key is absent.
    #[serde(deserialize_with = "super::model::deserialize_required_nullable")]
    pub current: Option<EvolutionCurrent>,
}

/// Revision-pinned exact command alias and semantic receipt read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionReceiptRead {
    /// Exact physical `StateRoot` revision observed by this read.
    pub observed_revision: String,
    /// All-ever command alias, or null when the exact key is absent.
    #[serde(deserialize_with = "super::model::deserialize_required_nullable")]
    pub alias: Option<EvolutionCommandAlias>,
    /// Stable semantic receipt selected by the alias, or null with an absent alias.
    #[serde(deserialize_with = "super::model::deserialize_required_nullable")]
    pub receipt: Option<EvolutionPersistenceReceipt>,
}

/// Non-persisted physical commit envelope returned by the closed Durable M4
/// façade. Physical revisions remain outside the content-addressed receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionCommit {
    /// Exact physical `StateRoot` revision observed when returning the receipt.
    pub observed_revision: String,
    /// Result revision for a new commit, or null for exact lost-ack replay.
    #[serde(deserialize_with = "super::model::deserialize_required_nullable")]
    pub committed_revision: Option<String>,
    /// Stable semantic receipt, identical on first commit and exact replay.
    pub receipt: EvolutionPersistenceReceipt,
}

/// Non-serializable exact authority view assembled from one immutable Durable
/// root. It contains only leaves named by the command's bounded read set.
#[derive(Debug)]
pub struct EvolutionAuthorityView {
    evolution_id: String,
    current: Option<EvolutionCurrent>,
    leaves: BTreeMap<(EvolutionStateFamily, String), EvolutionMutation>,
    lookups: BTreeSet<(EvolutionStateFamily, String)>,
    accounted_entries: BTreeMap<(EvolutionStateFamily, String), usize>,
    source_bytes: usize,
}

/// Pure reducer output consumed by Durable's one-CAS lowering.
#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionPostcondition {
    /// Resulting scalar partition current.
    pub current: EvolutionCurrent,
    /// Exact all-ever command alias to insert.
    pub alias: EvolutionCommandAlias,
    /// Exact all-ever semantic receipt to insert.
    pub receipt: EvolutionPersistenceReceipt,
    /// Strictly ordered normalized state mutations.
    pub mutations: Vec<EvolutionMutation>,
    /// Newly admitted executable Plans to retain atomically.
    pub plans: Vec<SealedPlan>,
    /// Newly introduced Artifact records to retain atomically.
    pub artifacts: Vec<ArtifactRecord>,
    /// Pre-existing Artifact references required by this transition.
    pub required_artifacts: BTreeSet<ArtifactRef>,
}

/// Closed typed M1 sidecar derived only from a verified migration
/// postcondition. Durable supplies actor and current-Run precondition authority
/// when it wraps this Core command in one atomic commit.
#[derive(Debug, Clone, PartialEq)]
pub struct EvolutionMigrationSidecar {
    command_id: String,
    run_id: String,
    command: cymule_core::Command,
    target_continuation: cymule_durable_protocol::Continuation,
}

impl EvolutionMigrationSidecar {
    /// Evolution persistence identity reused as the exact M1 command identity.
    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    /// Source Run whose exact current will supply the M1 precondition.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Closed Core migration command bound to the semantic receipt.
    pub fn command(&self) -> &cymule_core::Command {
        &self.command
    }

    /// Complete claim-free target Continuation committed beside the command.
    pub fn target_continuation(&self) -> &cymule_durable_protocol::Continuation {
        &self.target_continuation
    }
}

/// Non-serializable detached reduction prepared from one immutable root.
pub struct PreparedEvolution<'a> {
    view: &'a EvolutionAuthorityView,
    command: &'a EvolutionPersistenceCommand,
    revision: u64,
    parent_current_id: Option<String>,
    source: &'a EvolutionReductionSourceBody,
    source_witness_id: Option<&'a str>,
    reduction: PreparedReduction,
}

impl PreparedEvolution<'_> {
    /// Return the exact target Plan already loaded and deterministically
    /// validated for a prepared migration command.
    ///
    /// # Errors
    ///
    /// Returns an error only if the prepared view no longer contains the exact
    /// target Plan read that was required during preparation.
    pub fn migration_target_plan(&self) -> EvolutionResult<Option<&SealedPlan>> {
        let LiveEvolutionCommand::Apply {
            template_id,
            command,
            ..
        } = &self.command.command
        else {
            return Ok(None);
        };
        let EvolutionCommand::Migrate { request, .. } = command.as_ref() else {
            return Ok(None);
        };
        plan_record(
            self.view,
            &self.command.evolution_id,
            template_id,
            &request.to_plan,
        )?
        .map(|record| &record.plan)
        .ok_or_else(|| {
            EvolutionError::NotFound(
                "prepared migration target Plan is missing from its exact read view".to_owned(),
            )
        })
        .map(Some)
    }

    /// Return every pre-existing Artifact record whose exact membership must
    /// be proven at this pinned root before the selected provider may run.
    /// Deterministic commands and retained semantic replay return an empty set;
    /// newly materialized target-binding and provider-output records are not
    /// pre-existing membership requirements.
    ///
    /// # Errors
    ///
    /// Returns an error when a provider-required preparation lost its exact
    /// migration source authority or names an unsupported provider transition.
    pub fn provider_required_artifacts(&self) -> EvolutionResult<BTreeSet<ArtifactRef>> {
        if matches!(&self.reduction, PreparedReduction::Deterministic(_)) {
            return Ok(BTreeSet::new());
        }
        let LiveEvolutionCommand::Apply { command, .. } = &self.command.command else {
            return Err(EvolutionError::Integrity {
                code: "invalid_evolution_provider_preparation".to_owned(),
                message: "provider-required preparation is not one Apply command".to_owned(),
            });
        };
        match command.as_ref() {
            EvolutionCommand::Migrate { .. } => {
                let EvolutionReductionSourceBody::Migration {
                    continuation,
                    source_binding,
                    ..
                } = self.source
                else {
                    return Err(EvolutionError::Integrity {
                        code: "invalid_evolution_provider_preparation".to_owned(),
                        message: "fresh migration provider preparation lost its source authority"
                            .to_owned(),
                    });
                };
                let mut required = continuation_artifacts(continuation)?;
                required.insert(source_binding.clone());
                Ok(required)
            }
            EvolutionCommand::Shadow { request, .. } => Ok(BTreeSet::from([request.input.clone()])),
            _ => Err(EvolutionError::Integrity {
                code: "invalid_evolution_provider_preparation".to_owned(),
                message: "provider-required preparation selected a deterministic command"
                    .to_owned(),
            }),
        }
    }
}

/// Non-serializable occurrence selection prepared entirely from one pinned
/// Evolution view before Durable resolves the already-admitted M1 execution
/// binding.
#[derive(Debug)]
pub struct PreparedEvolutionSelection<'a> {
    view: &'a EvolutionAuthorityView,
    command: EvolutionPersistenceCommand,
    source_current: EvolutionCurrent,
    occurrence_id: String,
    selection_id: String,
    decision_id: String,
    plan: SealedPlan,
}

/// Exact deterministic migration target preparation completed before any
/// target binding or adapter provider lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedEvolutionMigrationTarget {
    plan: SealedPlan,
    retained_target_binding: Option<ArtifactRef>,
}

impl PreparedEvolutionMigrationTarget {
    /// Exact admitted target Plan.
    pub fn plan(&self) -> &SealedPlan {
        &self.plan
    }

    /// Target binding already retained by an exact semantic-record replay. A
    /// fresh migration returns `None` and requires fixed-registry resolution.
    pub fn retained_target_binding(&self) -> Option<&ArtifactRef> {
        self.retained_target_binding.as_ref()
    }
}

enum PreparedReduction {
    Deterministic(Box<ReducedEvolution>),
    ProviderRequired,
}

/// Non-serializable runtime authority derived by Durable from one pinned M1
/// root after it has verified its private quiescence receipt.
pub struct EvolutionReductionSource {
    body: EvolutionReductionSourceBody,
}

enum EvolutionReductionSourceBody {
    None,
    Selection {
        plan_id: String,
        execution_binding: ArtifactRecord,
    },
    Migration {
        safe_point: MigrationSafePoint,
        continuation: cymule_durable_protocol::Continuation,
        source_binding: ArtifactRef,
        target_binding: ArtifactRecord,
    },
    RetainedMigration {
        target_binding: ArtifactRecord,
    },
    Restart {
        safe_point: MigrationSafePoint,
        continuation: cymule_durable_protocol::Continuation,
    },
}

/// Verify one registry-resolved target binding against the exact prepared Plan
/// and materialize its canonical Artifact record for the final CAS.
///
/// # Errors
///
/// Returns an error when the Plan or binding is invalid, the binding does not
/// admit the Plan, or canonical Artifact material cannot be derived.
pub fn admit_evolution_target_binding(
    plan: &SealedPlan,
    binding: &cymule_runtime::ExecutionBinding,
) -> EvolutionResult<ArtifactRecord> {
    let bytes = binding.canonical_bytes().map_err(|error| {
        EvolutionError::Validation(format!(
            "target ExecutionBinding cannot derive canonical bytes: {error}"
        ))
    })?;
    let record = ArtifactRecord {
        reference: binding.artifact_ref().map_err(|error| {
            EvolutionError::Validation(format!(
                "target ExecutionBinding cannot derive its Artifact identity: {error}"
            ))
        })?,
        bytes,
    };
    verify_evolution_target_binding_record(plan, &record)?;
    Ok(record)
}

/// Verify a retained complete target binding record against its exact migration
/// Plan without consulting the provider registry.
///
/// # Errors
///
/// Returns an error when the Artifact is malformed or noncanonical, is not an
/// `ExecutionBinding`, or does not admit the exact Plan.
pub fn verify_evolution_target_binding_record(
    plan: &SealedPlan,
    record: &ArtifactRecord,
) -> EvolutionResult<()> {
    plan.verify()?;
    verify_artifact_record(record)?;
    verify_bounded(
        "target ExecutionBinding Artifact",
        record,
        MAX_EVOLUTION_LEAF_BYTES,
    )?;
    verify_execution_binding_ref(&record.reference)?;
    let binding: cymule_runtime::ExecutionBinding = cymule_core::decode_json(&record.bytes)
        .map_err(|error| {
            EvolutionError::Validation(format!(
                "target ExecutionBinding Artifact is not strict typed JSON: {error}"
            ))
        })?;
    binding.verify().map_err(|error| {
        EvolutionError::Validation(format!("target ExecutionBinding is invalid: {error}"))
    })?;
    binding.admit_plan(plan).map_err(|error| {
        EvolutionError::Conflict(format!(
            "target ExecutionBinding does not admit the migration Plan: {error}"
        ))
    })?;
    if binding.canonical_bytes().map_err(|error| {
        EvolutionError::Validation(format!(
            "target ExecutionBinding cannot derive canonical bytes: {error}"
        ))
    })? != record.bytes
    {
        return Err(EvolutionError::Validation(
            "target ExecutionBinding Artifact bytes are not canonical".to_owned(),
        ));
    }
    Ok(())
}

impl EvolutionReductionSource {
    /// Construct the source for a command that does not consume M1 Run state.
    pub fn none() -> Self {
        Self {
            body: EvolutionReductionSourceBody::None,
        }
    }

    /// Construct occurrence-selection authority from an exact binding already
    /// admitted for the selected Plan in the same pinned M1 root.
    ///
    /// # Errors
    ///
    /// Returns an error when the Plan, Artifact record, canonical typed
    /// binding, or Plan admission relation is invalid.
    pub fn selection(
        plan: &SealedPlan,
        execution_binding: ArtifactRecord,
    ) -> EvolutionResult<Self> {
        plan.verify()?;
        verify_artifact_record(&execution_binding)?;
        verify_bounded(
            "selected ExecutionBinding Artifact",
            &execution_binding,
            MAX_EVOLUTION_LEAF_BYTES,
        )?;
        verify_execution_binding_ref(&execution_binding.reference)?;
        let binding: cymule_runtime::ExecutionBinding =
            cymule_core::decode_json(&execution_binding.bytes).map_err(|error| {
                EvolutionError::Validation(format!(
                    "selected ExecutionBinding Artifact is not strict typed JSON: {error}"
                ))
            })?;
        binding.verify().map_err(|error| {
            EvolutionError::Validation(format!("selected ExecutionBinding is invalid: {error}"))
        })?;
        binding.admit_plan(plan).map_err(|error| {
            EvolutionError::Conflict(format!(
                "selected ExecutionBinding does not admit the selected Plan: {error}"
            ))
        })?;
        if binding.artifact_ref().map_err(|error| {
            EvolutionError::Validation(format!(
                "selected ExecutionBinding cannot derive its Artifact identity: {error}"
            ))
        })? != execution_binding.reference
        {
            return Err(EvolutionError::Validation(
                "selected ExecutionBinding Artifact bytes are not canonical".to_owned(),
            ));
        }
        Ok(Self {
            body: EvolutionReductionSourceBody::Selection {
                plan_id: plan.plan_id.clone(),
                execution_binding,
            },
        })
    }

    /// Construct migration authority from Durable-verified exact source state.
    ///
    /// # Errors
    ///
    /// Returns an error when the source proof, Continuation, source binding
    /// reference, or complete target binding record is malformed or inconsistent.
    pub fn migration(
        safe_point: MigrationSafePoint,
        continuation: cymule_durable_protocol::Continuation,
        source_binding: ArtifactRef,
        target_binding: ArtifactRecord,
    ) -> EvolutionResult<Self> {
        verify_reduction_source(&safe_point, &continuation)?;
        verify_execution_binding_ref(&source_binding)?;
        verify_artifact_record(&target_binding)?;
        verify_bounded(
            "target ExecutionBinding Artifact",
            &target_binding,
            MAX_EVOLUTION_LEAF_BYTES,
        )?;
        verify_execution_binding_ref(&target_binding.reference)?;
        if safe_point.binding_context != source_binding.artifact_id {
            return Err(EvolutionError::Conflict(
                "migration source binding does not match the exact source witness".to_owned(),
            ));
        }
        if safe_point.state.is_none() {
            return Err(EvolutionError::Conflict(
                "migration source witness has no state Artifact".to_owned(),
            ));
        }
        Ok(Self {
            body: EvolutionReductionSourceBody::Migration {
                safe_point,
                continuation,
                source_binding,
                target_binding,
            },
        })
    }

    /// Construct restart authority from Durable-verified exact source state.
    ///
    /// # Errors
    ///
    /// Returns an error when the source proof does not cover the supplied
    /// quiescent Continuation.
    pub fn restart(
        safe_point: MigrationSafePoint,
        continuation: cymule_durable_protocol::Continuation,
    ) -> EvolutionResult<Self> {
        verify_reduction_source(&safe_point, &continuation)?;
        Ok(Self {
            body: EvolutionReductionSourceBody::Restart {
                safe_point,
                continuation,
            },
        })
    }

    /// Construct exact semantic-record replay authority from the retained
    /// target binding Artifact. No obsolete Run safe point is revalidated.
    ///
    /// # Errors
    ///
    /// Returns an error when the retained target binding record is malformed or
    /// does not identify an `ExecutionBinding` Artifact. Exact typed decoding,
    /// canonical-byte verification, and Plan admission happen against the
    /// retained target Plan during preparation.
    pub fn retained_migration(target_binding: ArtifactRecord) -> EvolutionResult<Self> {
        verify_artifact_record(&target_binding)?;
        verify_bounded(
            "retained target ExecutionBinding Artifact",
            &target_binding,
            MAX_EVOLUTION_LEAF_BYTES,
        )?;
        verify_execution_binding_ref(&target_binding.reference)?;
        Ok(Self {
            body: EvolutionReductionSourceBody::RetainedMigration { target_binding },
        })
    }
}

/// Derive the one deterministic M4 selection identity owned by a Virtual
/// persistence command. Callers cannot choose a second cross-profile identity.
///
/// # Errors
///
/// Returns an error when the Virtual persistence identity is not a content ID.
pub fn derive_virtual_evolution_selection_id(
    virtual_persistence_id: &str,
) -> EvolutionResult<String> {
    verify_content_id("Virtual persistence command", virtual_persistence_id)?;
    content_id(
        VIRTUAL_EVOLUTION_SELECTION_ID_DOMAIN,
        &virtual_persistence_id,
    )
    .map_err(Into::into)
}

/// Prepare one generic occurrence selection from exact normalized M4 reads
/// before Durable resolves the selected binding Artifact bytes.
///
/// # Errors
///
/// Returns an error when the command is not `SelectOccurrence`, targets another
/// partition, has no scalar current, requires another exact read, or conflicts
/// with retained occurrence/selection lineage.
pub fn prepare_evolution_selection<'a>(
    view: &'a EvolutionAuthorityView,
    command: &EvolutionPersistenceCommand,
) -> EvolutionResult<PreparedEvolutionSelection<'a>> {
    command.verify()?;
    if view.evolution_id() != command.evolution_id {
        return Err(EvolutionError::Conflict(
            "occurrence selection targets a different pinned Evolution partition".to_owned(),
        ));
    }
    let LiveEvolutionCommand::Apply {
        template_id,
        command: inner,
        ..
    } = &command.command
    else {
        return Err(EvolutionError::Validation(
            "occurrence selection preparation requires one Apply command".to_owned(),
        ));
    };
    let EvolutionCommand::SelectOccurrence {
        occurrence_id,
        selection_id,
        ..
    } = inner.as_ref()
    else {
        return Err(EvolutionError::Validation(
            "occurrence selection preparation requires SelectOccurrence".to_owned(),
        ));
    };
    let source_current = view.current().cloned().ok_or_else(|| {
        EvolutionError::NotFound("occurrence selection has no Evolution scalar current".to_owned())
    })?;
    let lineage = occurrence_selection_lineage(
        view,
        &command.evolution_id,
        template_id,
        occurrence_id,
        selection_id,
    )?;
    Ok(PreparedEvolutionSelection {
        view,
        command: command.clone(),
        source_current,
        occurrence_id: occurrence_id.clone(),
        selection_id: selection_id.clone(),
        decision_id: lineage.decision_id,
        plan: lineage.plan,
    })
}

/// Prepare and return the exact target Plan for one migration before the fixed
/// provider registry resolves its target execution binding.
///
/// # Errors
///
/// Returns an error when the command is not `Migrate`, targets another
/// partition, requires another exact read, or fails deterministic Plan, edge,
/// compatibility, no-widening, or retained-record validation.
pub fn prepare_evolution_migration_target(
    view: &EvolutionAuthorityView,
    command: &EvolutionPersistenceCommand,
) -> EvolutionResult<PreparedEvolutionMigrationTarget> {
    command.verify()?;
    if view.evolution_id() != command.evolution_id {
        return Err(EvolutionError::Conflict(
            "migration targets a different pinned Evolution partition".to_owned(),
        ));
    }
    let LiveEvolutionCommand::Apply {
        template_id,
        command: inner,
        ..
    } = &command.command
    else {
        return Err(EvolutionError::Validation(
            "migration target preparation requires one Apply command".to_owned(),
        ));
    };
    let EvolutionCommand::Migrate { request, .. } = inner.as_ref() else {
        return Err(EvolutionError::Validation(
            "migration target preparation requires Migrate".to_owned(),
        ));
    };
    if view.current().is_none() {
        return Err(EvolutionError::NotFound(
            "migration target preparation has no Evolution scalar current".to_owned(),
        ));
    }
    if template_current(view, &command.evolution_id, template_id, "current")?.is_none() {
        return Err(EvolutionError::NotFound(format!(
            "live evolution template {template_id} is missing"
        )));
    }
    let (plan, retained) =
        migration_plan_preflight(view, &command.evolution_id, template_id, request)?;
    Ok(PreparedEvolutionMigrationTarget {
        plan: plan.clone(),
        retained_target_binding: retained.map(|record| record.receipt.target_binding.clone()),
    })
}

/// Prepare the complete exact M4 read set and selected Plan for one eligible
/// Virtual occurrence before Durable resolves its M1-admitted binding.
///
/// # Errors
///
/// Returns an error for Direct execution, malformed or cross-partition input,
/// missing exact leaves, or conflicting retained occurrence lineage.
pub fn prepare_virtual_evolution_selection<'a>(
    view: &'a EvolutionAuthorityView,
    run_execution: &crate::virtual_work::VirtualRunExecution,
    virtual_persistence_id: &str,
    occurrence_id: &str,
    execution_binding: &ArtifactRef,
) -> EvolutionResult<PreparedEvolutionSelection<'a>> {
    run_execution
        .verify()
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    let crate::virtual_work::VirtualRunExecution::Evolution {
        evolution_id,
        template_id,
    } = run_execution
    else {
        return Err(EvolutionError::Conflict(
            "Direct Virtual Run execution has no M4 selection transition".to_owned(),
        ));
    };
    validate_identity("Virtual evolution authority", evolution_id)?;
    validate_identity("Virtual evolution template", template_id)?;
    validate_identity("Virtual work occurrence", occurrence_id)?;
    verify_execution_binding_ref(execution_binding)?;
    if view.evolution_id() != evolution_id {
        return Err(EvolutionError::Conflict(
            "Virtual selection targets a different pinned Evolution partition".to_owned(),
        ));
    }
    let selection_id = derive_virtual_evolution_selection_id(virtual_persistence_id)?;
    let inner = EvolutionCommand::SelectOccurrence {
        control_version: super::EVOLUTION_CONTROL_VERSION.to_owned(),
        command_id: selection_id.clone(),
        occurrence_id: occurrence_id.to_owned(),
        selection_id: selection_id.clone(),
        execution_binding: execution_binding.clone(),
    };
    let command = EvolutionPersistenceCommand::new(
        evolution_id.to_owned(),
        LiveEvolutionCommand::Apply {
            control_version: super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: selection_id.clone(),
            template_id: template_id.to_owned(),
            command: Box::new(inner),
        },
    )?;
    prepare_evolution_selection(view, &command)
}

impl PreparedEvolutionSelection<'_> {
    /// Exact semantic command whose alias and receipt will be committed.
    pub fn command(&self) -> &EvolutionPersistenceCommand {
        &self.command
    }

    /// Scalar Evolution current from which the selection was prepared.
    pub fn source_current(&self) -> &EvolutionCurrent {
        &self.source_current
    }

    /// Exact selection identity retained by the prepared command.
    pub fn selection_id(&self) -> &str {
        &self.selection_id
    }

    /// Exact occurrence selected by the prepared command.
    pub fn occurrence_id(&self) -> &str {
        &self.occurrence_id
    }

    /// Exact rollout decision used for deterministic selection.
    pub fn decision_id(&self) -> &str {
        &self.decision_id
    }

    /// Exact admitted semantic Plan selected before binding resolution.
    pub fn plan(&self) -> &SealedPlan {
        &self.plan
    }
}

/// Finish one prepared occurrence selection using the exact
/// `ExecutionBinding` derived from M1 authority at the same pinned root.
///
/// # Errors
///
/// Returns an error when the loaded binding bytes are not the requested exact
/// canonical binding, do not admit the selected Plan, or the final reducer
/// changes any prepared lineage.
pub fn reduce_evolution_selection(
    prepared: PreparedEvolutionSelection<'_>,
    execution_binding: ArtifactRecord,
) -> EvolutionResult<EvolutionPostcondition> {
    let PreparedEvolutionSelection {
        view,
        command,
        source_current: _,
        occurrence_id,
        selection_id,
        decision_id,
        plan,
    } = prepared;
    let execution_binding_ref = execution_binding.reference.clone();
    let source = EvolutionReductionSource::selection(&plan, execution_binding)?;
    let transition = prepare_evolution(view, &command, &source)?;
    let provider = execute_evolution_provider(&transition, &mut NoEvolutionProviders)?;
    let postcondition = reduce_prepared_evolution(transition, provider)?;
    let LiveEvolutionOutcome::OccurrenceSelected { pin } = &postcondition.receipt.outcome else {
        return Err(EvolutionError::Conflict(
            "prepared selection produced a non-selection Evolution outcome".to_owned(),
        ));
    };
    if pin.occurrence_id != occurrence_id
        || pin.selection_id != selection_id
        || pin.decision_id != decision_id
        || pin.plan_id != plan.plan_id
        || pin.execution_binding != execution_binding_ref
    {
        return Err(EvolutionError::Conflict(
            "selection result changed its prepared exact lineage".to_owned(),
        ));
    }
    Ok(postcondition)
}

fn verify_reduction_source(
    safe_point: &MigrationSafePoint,
    continuation: &cymule_durable_protocol::Continuation,
) -> EvolutionResult<()> {
    safe_point.verify()?;
    safe_point.verify_source_continuation(continuation)?;
    continuation
        .verify_wire()
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    super::adapters::verify_continuation_safe_integers(continuation)?;
    if continuation.status != cymule_durable_protocol::ContinuationStatus::Ready
        || continuation.execution_claim.is_some()
        || continuation.frames.is_empty()
        || !continuation.wait_set.is_empty()
        || continuation.scope_stack != [cymule_core::ROOT_SCOPE_ID]
    {
        return Err(EvolutionError::Conflict(
            "evolution source is not a quiescent Ready Continuation".to_owned(),
        ));
    }
    Ok(())
}

fn verify_execution_binding_ref(reference: &ArtifactRef) -> EvolutionResult<()> {
    reference
        .validate()
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    if reference.kind != cymule_runtime::EXECUTION_BINDING_VERSION {
        return Err(EvolutionError::Validation(
            "evolution runtime source requires an exact ExecutionBinding Artifact".to_owned(),
        ));
    }
    Ok(())
}

/// Opaque non-serializable provider authority produced only by executing the
/// provider selected for a prepared migration or shadow command.
pub struct EvolutionProviderAuthority {
    persistence_id: String,
    body: EvolutionProviderAuthorityBody,
}

enum EvolutionProviderAuthorityBody {
    None,
    Migration(Box<MigrationProviderAuthority>),
    Shadow(Box<ShadowProviderAuthority>),
}

struct MigrationProviderAuthority {
    receipt: MigrationReceipt,
    artifacts: Vec<ArtifactRecord>,
}

struct ShadowProviderAuthority {
    comparison: ShadowComparison,
    evidence: ArtifactRecord,
}

/// Exact provider registry used only after detached preparation has completed.
/// Implementations must resolve the semantic identity and immutable revision
/// supplied by the command; provider descriptors are checked again before any
/// product can enter the postcondition.
pub trait EvolutionProviders {
    /// Resolve the complete immutable target execution binding selected for one
    /// exact migration Plan.
    ///
    /// # Errors
    ///
    /// Returns an error when no exact binding authority exists for that Plan.
    fn target_execution_binding(
        &mut self,
        plan_id: &str,
    ) -> EvolutionResult<cymule_runtime::ExecutionBinding>;

    /// Resolve one exact admitted migration adapter.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact identity and revision are unavailable.
    fn migration_adapter(
        &mut self,
        adapter_id: &str,
        adapter_revision: &str,
    ) -> EvolutionResult<&mut dyn MigrationAdapter>;

    /// Resolve one exact admitted shadow driver.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact identity and revision are unavailable.
    fn shadow_driver(
        &mut self,
        driver_id: &str,
        driver_revision: &str,
    ) -> EvolutionResult<&mut dyn ShadowDriver>;
}

/// Empty provider registry for commands whose semantic reducer invokes no
/// external implementation.
#[derive(Debug, Default)]
pub struct NoEvolutionProviders;

impl EvolutionProviders for NoEvolutionProviders {
    fn target_execution_binding(
        &mut self,
        plan_id: &str,
    ) -> EvolutionResult<cymule_runtime::ExecutionBinding> {
        Err(EvolutionError::NotFound(format!(
            "target execution binding for Plan {plan_id} is not registered"
        )))
    }

    fn migration_adapter(
        &mut self,
        adapter_id: &str,
        adapter_revision: &str,
    ) -> EvolutionResult<&mut dyn MigrationAdapter> {
        Err(EvolutionError::NotFound(format!(
            "migration adapter {adapter_id}@{adapter_revision} is not registered"
        )))
    }

    fn shadow_driver(
        &mut self,
        driver_id: &str,
        driver_revision: &str,
    ) -> EvolutionResult<&mut dyn ShadowDriver> {
        Err(EvolutionError::NotFound(format!(
            "shadow driver {driver_id}@{driver_revision} is not registered"
        )))
    }
}

/// Purely validate a semantic command against its exact bounded read view.
/// Provider code is not invoked in this phase.
///
/// # Errors
///
/// Returns an error when the command, pinned source, required exact reads, or
/// deterministic semantic preconditions are invalid.
pub fn prepare_evolution<'a>(
    view: &'a EvolutionAuthorityView,
    command: &'a EvolutionPersistenceCommand,
    source: &'a EvolutionReductionSource,
) -> EvolutionResult<PreparedEvolution<'a>> {
    command.verify()?;
    if view.evolution_id != command.evolution_id {
        return Err(EvolutionError::Conflict(
            "evolution command targets a different pinned authority view".to_owned(),
        ));
    }
    verify_reduction_source_aggregate(view, &source.body)?;
    let (revision, parent_current_id) = match &view.current {
        Some(current) => {
            current.verify()?;
            if current.evolution_id != command.evolution_id {
                return Err(EvolutionError::Conflict(
                    "evolution command targets a different scalar current".to_owned(),
                ));
            }
            let revision = current
                .revision
                .checked_add(1)
                .filter(|revision| *revision <= cymule_core::MAX_EXACT_INTEGER)
                .ok_or_else(|| {
                    EvolutionError::Validation(
                        "evolution revision exhausted the JSON exact-integer range".to_owned(),
                    )
                })?;
            (revision, Some(current.current_id.clone()))
        }
        None => (1, None),
    };
    prevalidate_command(view, command, &source.body)?;
    let reduction = if command_requires_provider(view, command)? {
        PreparedReduction::ProviderRequired
    } else {
        PreparedReduction::Deterministic(Box::new(reduce_command(
            view,
            command,
            revision,
            &source.body,
            EvolutionProviderAuthorityBody::None,
        )?))
    };
    let source_witness_id = match &source.body {
        EvolutionReductionSourceBody::None | EvolutionReductionSourceBody::Selection { .. } => None,
        EvolutionReductionSourceBody::Migration { safe_point, .. }
        | EvolutionReductionSourceBody::Restart { safe_point, .. } => {
            Some(safe_point.safe_point_id.as_str())
        }
        EvolutionReductionSourceBody::RetainedMigration { .. } => Some(
            retained_migration_record(view, command)?
                .receipt
                .source_witness_id
                .as_str(),
        ),
    };
    Ok(PreparedEvolution {
        view,
        command,
        revision,
        parent_current_id,
        source: &source.body,
        source_witness_id,
        reduction,
    })
}

fn verify_reduction_source_aggregate(
    view: &EvolutionAuthorityView,
    source: &EvolutionReductionSourceBody,
) -> EvolutionResult<()> {
    let authority_bytes = match source {
        EvolutionReductionSourceBody::None => 0,
        EvolutionReductionSourceBody::Selection {
            plan_id,
            execution_binding,
        } => canonical_bytes(&(plan_id, execution_binding))?.len(),
        EvolutionReductionSourceBody::Migration {
            safe_point,
            continuation,
            source_binding,
            target_binding,
        } => canonical_bytes(&(
            safe_point.safe_point_version.as_str(),
            safe_point.safe_point_id.as_str(),
            safe_point.domain_revision.as_str(),
            safe_point.run_id.as_str(),
            safe_point.plan_id.as_str(),
            safe_point.binding_context.as_str(),
            safe_point.epoch,
            &safe_point.state,
            safe_point.continuation_digest.as_str(),
            continuation,
            source_binding,
            target_binding,
        ))?
        .len(),
        EvolutionReductionSourceBody::RetainedMigration { target_binding } => {
            canonical_bytes(target_binding)?.len()
        }
        EvolutionReductionSourceBody::Restart {
            safe_point,
            continuation,
        } => canonical_bytes(&(
            safe_point.safe_point_version.as_str(),
            safe_point.safe_point_id.as_str(),
            safe_point.domain_revision.as_str(),
            safe_point.run_id.as_str(),
            safe_point.plan_id.as_str(),
            safe_point.binding_context.as_str(),
            safe_point.epoch,
            &safe_point.state,
            safe_point.continuation_digest.as_str(),
            continuation,
        ))?
        .len(),
    };
    let total = view
        .source_bytes()
        .checked_add(authority_bytes)
        .ok_or_else(|| {
            EvolutionError::Validation(
                "evolution reducer source byte accounting overflowed".to_owned(),
            )
        })?;
    if total > MAX_EVOLUTION_SOURCE_BYTES {
        return Err(EvolutionError::Validation(format!(
            "evolution reducer source uses {total} canonical-accounted bytes, above the {MAX_EVOLUTION_SOURCE_BYTES} byte bound"
        )));
    }
    Ok(())
}

/// Invoke only the provider required by a prepared command and return an
/// opaque authority token. Exact replay lookup must happen before this call.
///
/// # Errors
///
/// Returns an error when the exact registered provider is unavailable, its
/// descriptor differs from the command, or its closed product is invalid.
pub fn execute_evolution_provider(
    prepared: &PreparedEvolution<'_>,
    providers: &mut dyn EvolutionProviders,
) -> EvolutionResult<EvolutionProviderAuthority> {
    if matches!(&prepared.reduction, PreparedReduction::Deterministic(_)) {
        return Ok(EvolutionProviderAuthority {
            persistence_id: prepared.command.persistence_id.clone(),
            body: EvolutionProviderAuthorityBody::None,
        });
    }
    let body = match &prepared.command.command {
        LiveEvolutionCommand::Apply {
            template_id,
            command,
            ..
        } => match command.as_ref() {
            EvolutionCommand::Migrate { request, .. } => {
                if migration_record(
                    prepared.view,
                    &prepared.command.evolution_id,
                    template_id,
                    &request.migration_id,
                )?
                .is_some()
                {
                    EvolutionProviderAuthorityBody::None
                } else {
                    let target = plan_record(
                        prepared.view,
                        &prepared.command.evolution_id,
                        template_id,
                        &request.to_plan,
                    )?
                    .ok_or_else(|| {
                        EvolutionError::NotFound(
                            "migration target Plan is missing from the exact read view".to_owned(),
                        )
                    })?;
                    let adapter = providers
                        .migration_adapter(&request.adapter_id, &request.adapter_revision)?;
                    let EvolutionReductionSourceBody::Migration {
                        safe_point,
                        continuation,
                        source_binding,
                        target_binding,
                    } = prepared.source
                    else {
                        return Err(EvolutionError::Validation(
                            "migration command has no Durable-derived runtime source".to_owned(),
                        ));
                    };
                    let adapter_request = migration_adapter_request(
                        request,
                        safe_point,
                        continuation,
                        source_binding,
                        target_binding,
                    )?;
                    let (receipt, artifacts) = super::controller::execute_migration_product(
                        adapter,
                        adapter_request,
                        &target.plan,
                    )?;
                    EvolutionProviderAuthorityBody::Migration(Box::new(
                        MigrationProviderAuthority { receipt, artifacts },
                    ))
                }
            }
            EvolutionCommand::Shadow { request, .. } => {
                if shadow_record(
                    prepared.view,
                    &prepared.command.evolution_id,
                    template_id,
                    &request.comparison_id,
                )?
                .is_some()
                {
                    EvolutionProviderAuthorityBody::None
                } else {
                    let driver =
                        providers.shadow_driver(&request.driver_id, &request.driver_revision)?;
                    let (comparison, evidence) =
                        super::controller::execute_shadow_product(driver, request)?;
                    EvolutionProviderAuthorityBody::Shadow(Box::new(ShadowProviderAuthority {
                        comparison,
                        evidence,
                    }))
                }
            }
            _ => EvolutionProviderAuthorityBody::None,
        },
        _ => EvolutionProviderAuthorityBody::None,
    };
    Ok(EvolutionProviderAuthority {
        persistence_id: prepared.command.persistence_id.clone(),
        body,
    })
}

/// Complete a prepared transition using its exact opaque provider authority.
/// This phase is pure and returns only bounded typed mutations; Durable owns
/// the subsequent single CAS and its physical result envelope.
///
/// # Errors
///
/// Returns an error when the authority belongs to another command or the
/// resulting semantic postcondition fails exact closure validation.
pub fn reduce_prepared_evolution(
    prepared: PreparedEvolution<'_>,
    provider: EvolutionProviderAuthority,
) -> EvolutionResult<EvolutionPostcondition> {
    if provider.persistence_id != prepared.command.persistence_id {
        return Err(EvolutionError::Conflict(
            "provider authority belongs to a different evolution command".to_owned(),
        ));
    }
    let reduced = match prepared.reduction {
        PreparedReduction::Deterministic(reduced) => {
            require_no_provider(&provider.body)?;
            *reduced
        }
        PreparedReduction::ProviderRequired => reduce_command(
            prepared.view,
            prepared.command,
            prepared.revision,
            prepared.source,
            provider.body,
        )?,
    };
    finish_postcondition(
        prepared.command.clone(),
        prepared.revision,
        prepared.parent_current_id,
        prepared.source_witness_id.map(str::to_owned),
        reduced,
    )
}

struct ReducedEvolution {
    outcome: LiveEvolutionOutcome,
    mutations: Vec<EvolutionMutation>,
    plans: Vec<SealedPlan>,
    artifacts: Vec<ArtifactRecord>,
    required_artifacts: BTreeSet<ArtifactRef>,
}

impl EvolutionPersistenceCommand {
    /// Seal one semantic command with its content-derived persistence identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the partition or semantic command is invalid or
    /// exceeds its canonical byte bound.
    pub fn new(
        evolution_id: impl Into<String>,
        command: LiveEvolutionCommand,
    ) -> EvolutionResult<Self> {
        let mut value = Self {
            persistence_version: EVOLUTION_PERSISTENCE_COMMAND_VERSION.to_owned(),
            persistence_id: String::new(),
            evolution_id: evolution_id.into(),
            command,
        };
        value.persistence_id = value.derived_id()?;
        value.verify()?;
        Ok(value)
    }

    /// Verify command generation, semantic intent, identity, and byte bound.
    ///
    /// # Errors
    ///
    /// Returns an error when any command invariant or canonical byte bound is
    /// violated.
    pub fn verify(&self) -> EvolutionResult<()> {
        if self.persistence_version != EVOLUTION_PERSISTENCE_COMMAND_VERSION {
            return Err(EvolutionError::Validation(format!(
                "unsupported evolution persistence command version {}",
                self.persistence_version
            )));
        }
        validate_identity("evolution authority", &self.evolution_id)?;
        self.command.verify()?;
        if self.persistence_id != self.derived_id()? {
            return Err(EvolutionError::Validation(
                "evolution persistence command identity does not match its semantic content"
                    .to_owned(),
            ));
        }
        verify_bounded(
            "evolution persistence command",
            self,
            MAX_EVOLUTION_COMMAND_BYTES,
        )
    }

    fn derived_id(&self) -> EvolutionResult<String> {
        content_id(
            EVOLUTION_PERSISTENCE_COMMAND_VERSION,
            &(
                self.persistence_version.as_str(),
                self.evolution_id.as_str(),
                &self.command,
            ),
        )
        .map_err(Into::into)
    }
}

impl EvolutionCurrentQuery {
    /// Verify the partition identity and optional exact physical revision.
    ///
    /// # Errors
    ///
    /// Returns an error when either identity is malformed.
    pub fn verify(&self) -> EvolutionResult<()> {
        validate_identity("evolution authority", &self.evolution_id)?;
        verify_optional_physical_revision(self.expected_revision.as_deref())
    }
}

impl EvolutionReceiptQuery {
    /// Verify the partition, command identity, and optional physical revision.
    ///
    /// # Errors
    ///
    /// Returns an error when any exact query identity is malformed.
    pub fn verify(&self) -> EvolutionResult<()> {
        validate_identity("evolution authority", &self.evolution_id)?;
        validate_identity("live-evolution command", &self.command_id)?;
        verify_optional_physical_revision(self.expected_revision.as_deref())
    }
}

impl EvolutionCurrentRead {
    /// Verify revision pinning and exact scalar-current ownership.
    ///
    /// # Errors
    ///
    /// Returns an error when the response changed its query revision or key,
    /// contains an invalid current, or exceeds its byte bound.
    pub fn verify_for(&self, query: &EvolutionCurrentQuery) -> EvolutionResult<()> {
        query.verify()?;
        verify_observed_revision(&self.observed_revision, query.expected_revision.as_deref())?;
        if let Some(current) = &self.current {
            current.verify()?;
            if current.evolution_id != query.evolution_id {
                return Err(EvolutionError::Conflict(
                    "evolution current read changed its exact partition key".to_owned(),
                ));
            }
        }
        verify_bounded("evolution current read", self, MAX_EVOLUTION_LEAF_BYTES)
    }
}

impl EvolutionReceiptRead {
    /// Verify revision pinning and exact alias-to-receipt ownership.
    ///
    /// # Errors
    ///
    /// Returns an error when the response changed its query revision or key,
    /// or when the alias, receipt, and command do not form one exact binding.
    pub fn verify_for(&self, query: &EvolutionReceiptQuery) -> EvolutionResult<()> {
        query.verify()?;
        verify_observed_revision(&self.observed_revision, query.expected_revision.as_deref())?;
        match (&self.alias, &self.receipt) {
            (None, None) => {}
            (Some(alias), Some(receipt)) => {
                alias.verify()?;
                receipt.verify()?;
                if alias.evolution_id != query.evolution_id
                    || alias.command_id != query.command_id
                    || alias.receipt_id != receipt.receipt_id
                    || alias.persistence_id != receipt.command.persistence_id
                    || alias.evolution_id != receipt.command.evolution_id
                    || alias.command_id != receipt.command.command.command_id()
                {
                    return Err(EvolutionError::Conflict(
                        "evolution receipt read changed its exact key or retained binding"
                            .to_owned(),
                    ));
                }
            }
            _ => {
                return Err(EvolutionError::Conflict(
                    "evolution receipt read contains an alias without its receipt".to_owned(),
                ));
            }
        }
        verify_bounded("evolution receipt read", self, MAX_EVOLUTION_LEAF_BYTES)
    }
}

impl EvolutionCommit {
    /// Verify one physical commit or exact replay envelope for a semantic command.
    ///
    /// # Errors
    ///
    /// Returns an error when the physical revisions are malformed or the
    /// semantic receipt does not bind the exact submitted command.
    pub fn verify_for(&self, command: &EvolutionPersistenceCommand) -> EvolutionResult<()> {
        command.verify()?;
        verify_content_id(
            "evolution observed StateRoot revision",
            &self.observed_revision,
        )?;
        if let Some(committed) = &self.committed_revision {
            verify_content_id("evolution committed StateRoot revision", committed)?;
            if committed != &self.observed_revision {
                return Err(EvolutionError::Conflict(
                    "new evolution commit did not return its resulting observed revision"
                        .to_owned(),
                ));
            }
        }
        self.receipt.verify()?;
        if &self.receipt.command != command {
            return Err(EvolutionError::Conflict(
                "evolution commit receipt belongs to a different semantic command".to_owned(),
            ));
        }
        verify_bounded("evolution commit envelope", self, MAX_EVOLUTION_LEAF_BYTES)
    }
}

fn verify_optional_physical_revision(revision: Option<&str>) -> EvolutionResult<()> {
    if let Some(revision) = revision {
        verify_content_id("evolution StateRoot revision", revision)?;
    }
    Ok(())
}

fn verify_observed_revision(observed: &str, expected: Option<&str>) -> EvolutionResult<()> {
    verify_content_id("evolution observed StateRoot revision", observed)?;
    if expected.is_some_and(|expected| expected != observed) {
        return Err(EvolutionError::Conflict(
            "evolution read revision does not match its exact query constraint".to_owned(),
        ));
    }
    Ok(())
}

impl EvolutionAuthorityView {
    /// Begin an exact bounded read view at one pinned scalar current.
    ///
    /// # Errors
    ///
    /// Returns an error when the partition/current is invalid, mismatched, or
    /// already exceeds the aggregate source bound.
    pub fn new(
        evolution_id: impl Into<String>,
        current: Option<EvolutionCurrent>,
    ) -> EvolutionResult<Self> {
        let evolution_id = evolution_id.into();
        validate_identity("evolution authority", &evolution_id)?;
        if let Some(current) = &current {
            current.verify()?;
            if current.evolution_id != evolution_id {
                return Err(EvolutionError::Conflict(
                    "evolution scalar current belongs to a different partition".to_owned(),
                ));
            }
        }
        let current_key = evolution_current_key(&evolution_id)?;
        let source_bytes = canonical_bytes(&("current", current_key, &current))?.len();
        if source_bytes > MAX_EVOLUTION_SOURCE_BYTES {
            return Err(EvolutionError::Validation(
                "evolution scalar current exceeds the reducer source byte bound".to_owned(),
            ));
        }
        Ok(Self {
            evolution_id,
            current,
            leaves: BTreeMap::new(),
            lookups: BTreeSet::new(),
            accounted_entries: BTreeMap::new(),
            source_bytes,
        })
    }

    /// Return the exact scalar current loaded from the pinned Durable root.
    pub fn current(&self) -> Option<&EvolutionCurrent> {
        self.current.as_ref()
    }

    /// Return the exact M4 authority partition pinned by this view.
    pub fn evolution_id(&self) -> &str {
        &self.evolution_id
    }

    /// Insert one exact leaf loaded from the same immutable root.
    ///
    /// # Errors
    ///
    /// Returns an error when the leaf is invalid, belongs to another partition
    /// or future revision, conflicts with an existing read, or exceeds a bound.
    pub fn insert(&mut self, mutation: EvolutionMutation) -> EvolutionResult<()> {
        mutation.verify()?;
        let key = mutation.storage_key()?;
        match &self.current {
            Some(current)
                if mutation.evolution_id() == current.evolution_id
                    && mutation.revision() <= current.revision => {}
            Some(_) => {
                return Err(EvolutionError::Conflict(
                    "evolution leaf does not belong to the scalar current".to_owned(),
                ));
            }
            None => {
                return Err(EvolutionError::Conflict(
                    "genesis evolution source cannot contain retained leaves".to_owned(),
                ));
            }
        }
        if let Some(existing) = self.leaves.get(&key) {
            return if existing == &mutation {
                Ok(())
            } else {
                Err(EvolutionError::Conflict(
                    "evolution authority view contains conflicting exact leaves".to_owned(),
                ))
            };
        }
        if !self.lookups.contains(&key) && self.lookups.len() == MAX_EVOLUTION_TRANSITION_LEAVES {
            return Err(EvolutionError::Validation(
                "evolution authority view exceeds the bounded transition read set".to_owned(),
            ));
        }
        let new_bytes = source_entry_bytes(key.0, &key.1, Some(&mutation))?;
        self.replace_source_accounting(&key, new_bytes)?;
        self.lookups.insert(key.clone());
        self.leaves.insert(key, mutation);
        Ok(())
    }

    /// Record authenticated non-membership for one exact key at the same root.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is invalid, contradicts a loaded member,
    /// or exceeds the read-count or aggregate-byte bound.
    pub fn record_missing(
        &mut self,
        family: EvolutionStateFamily,
        storage_key: String,
    ) -> EvolutionResult<()> {
        verify_content_id("evolution state lookup", &storage_key)?;
        if self.leaves.contains_key(&(family, storage_key.clone())) {
            return Err(EvolutionError::Conflict(
                "evolution authority view marks a retained leaf as missing".to_owned(),
            ));
        }
        let key = (family, storage_key);
        if self.lookups.contains(&key) {
            return Ok(());
        }
        if self.lookups.len() == MAX_EVOLUTION_TRANSITION_LEAVES {
            return Err(EvolutionError::Validation(
                "evolution authority view exceeds the bounded transition read set".to_owned(),
            ));
        }
        let new_bytes = source_entry_bytes(family, &key.1, None)?;
        self.replace_source_accounting(&key, new_bytes)?;
        self.lookups.insert(key);
        Ok(())
    }

    /// Return one previously loaded exact leaf without initiating a read.
    pub fn get(
        &self,
        family: EvolutionStateFamily,
        storage_key: &str,
    ) -> Option<&EvolutionMutation> {
        self.leaves.get(&(family, storage_key.to_owned()))
    }

    /// Return the number of loaded member leaves.
    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    /// Report whether the view contains no member leaves.
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    /// Canonical byte accounting accumulated before reducer preparation.
    pub fn source_bytes(&self) -> usize {
        self.source_bytes
    }

    fn replace_source_accounting(
        &mut self,
        key: &(EvolutionStateFamily, String),
        new_bytes: usize,
    ) -> EvolutionResult<()> {
        let old_bytes = self.accounted_entries.get(key).copied().unwrap_or(0);
        let without_old = self.source_bytes.checked_sub(old_bytes).ok_or_else(|| {
            EvolutionError::Validation("evolution source byte accounting underflowed".to_owned())
        })?;
        let total = without_old.checked_add(new_bytes).ok_or_else(|| {
            EvolutionError::Validation("evolution source byte accounting overflowed".to_owned())
        })?;
        if total > MAX_EVOLUTION_SOURCE_BYTES {
            return Err(EvolutionError::Validation(format!(
                "evolution reducer source uses {total} canonical-accounted bytes, above the {MAX_EVOLUTION_SOURCE_BYTES} byte bound"
            )));
        }
        self.accounted_entries.insert(key.clone(), new_bytes);
        self.source_bytes = total;
        Ok(())
    }

    fn lookup(
        &self,
        family: EvolutionStateFamily,
        storage_key: String,
    ) -> EvolutionResult<Option<&EvolutionMutation>> {
        if !self.lookups.contains(&(family, storage_key.clone())) {
            return Err(EvolutionError::ReadRequired {
                family,
                storage_key,
            });
        }
        Ok(self.leaves.get(&(family, storage_key)))
    }
}

fn source_entry_bytes(
    family: EvolutionStateFamily,
    storage_key: &str,
    mutation: Option<&EvolutionMutation>,
) -> EvolutionResult<usize> {
    canonical_bytes(&(family, storage_key, mutation))
        .map(|bytes| bytes.len())
        .map_err(Into::into)
}

impl EvolutionCurrent {
    /// Verify scalar-current generation, identity, revision, and byte bound.
    ///
    /// # Errors
    ///
    /// Returns an error when any scalar-current invariant is violated.
    pub fn verify(&self) -> EvolutionResult<()> {
        if self.current_version != EVOLUTION_CURRENT_VERSION
            || self.revision == 0
            || self.revision > cymule_core::MAX_EXACT_INTEGER
        {
            return Err(EvolutionError::Validation(
                "evolution scalar current has an invalid version or revision".to_owned(),
            ));
        }
        validate_identity("evolution authority", &self.evolution_id)?;
        verify_content_id("evolution receipt", &self.last_receipt_id)?;
        let expected = content_id(
            EVOLUTION_CURRENT_VERSION,
            &(
                self.current_version.as_str(),
                self.evolution_id.as_str(),
                self.revision,
                self.last_receipt_id.as_str(),
            ),
        )?;
        if self.current_id != expected {
            return Err(EvolutionError::Validation(
                "evolution scalar current identity does not match its content".to_owned(),
            ));
        }
        verify_bounded("evolution scalar current", self, MAX_EVOLUTION_LEAF_BYTES)
    }
}

impl EvolutionMutation {
    /// Return the exact M4 partition owned by this normalized mutation.
    pub fn evolution_id(&self) -> &str {
        match self {
            Self::DefinitionCurrent(value) | Self::DefinitionRecord(value) => &value.evolution_id,
            Self::DefinitionCompatibilityCurrent(value) => &value.evolution_id,
            Self::DependencyCurrent(value) => &value.evolution_id,
            Self::TemplateCurrent(value) => &value.evolution_id,
            Self::LinkRecord(value) => &value.evolution_id,
            Self::PlanRecord(value) => &value.evolution_id,
            Self::EdgeRecord(value) => &value.evolution_id,
            Self::RolloutCurrent(value) => &value.evolution_id,
            Self::RolloutEvidenceCurrent(value) => &value.evolution_id,
            Self::RolloutDecision(value) => &value.evolution_id,
            Self::OccurrenceCurrent(value) => &value.evolution_id,
            Self::SelectionCurrent(value) => &value.evolution_id,
            Self::MigrationRecord(value) => &value.evolution_id,
            Self::RestartRecord(value) => &value.evolution_id,
            Self::ShadowRecord(value) => &value.evolution_id,
            Self::ShadowSubjectCurrent(value) => &value.evolution_id,
            Self::ObservationRecord(value) => &value.evolution_id,
            Self::ObservationOccurrenceCurrent(value) => &value.evolution_id,
            Self::EvidenceCurrent(value) => &value.evolution_id,
            Self::DecisionTransitionCurrent(value) => &value.evolution_id,
            Self::TransitionRecord(value) => &value.evolution_id,
        }
    }

    /// Return the semantic partition revision produced by this mutation.
    pub fn revision(&self) -> u64 {
        match self {
            Self::DefinitionCurrent(value) | Self::DefinitionRecord(value) => value.revision,
            Self::DefinitionCompatibilityCurrent(value) => value.revision,
            Self::DependencyCurrent(value) => value.revision,
            Self::TemplateCurrent(value) => value.revision,
            Self::LinkRecord(value) => value.revision,
            Self::PlanRecord(value) => value.revision,
            Self::EdgeRecord(value) => value.revision,
            Self::RolloutCurrent(value) => value.revision,
            Self::RolloutEvidenceCurrent(value) => value.revision,
            Self::RolloutDecision(value) => value.revision,
            Self::OccurrenceCurrent(value) => value.revision,
            Self::SelectionCurrent(value) => value.revision,
            Self::MigrationRecord(value) => value.revision,
            Self::RestartRecord(value) => value.revision,
            Self::ShadowRecord(value) => value.revision,
            Self::ShadowSubjectCurrent(value) => value.revision,
            Self::ObservationRecord(value) => value.revision,
            Self::ObservationOccurrenceCurrent(value) => value.revision,
            Self::EvidenceCurrent(value) => value.revision,
            Self::DecisionTransitionCurrent(value) => value.revision,
            Self::TransitionRecord(value) => value.revision,
        }
    }

    /// Return the normalized `StateRoot` family for this mutation.
    pub fn family(&self) -> EvolutionStateFamily {
        match self {
            Self::DefinitionCurrent(_) => EvolutionStateFamily::DefinitionCurrent,
            Self::DefinitionCompatibilityCurrent(_) => {
                EvolutionStateFamily::DefinitionCompatibilityCurrent
            }
            Self::DefinitionRecord(_) => EvolutionStateFamily::DefinitionRecord,
            Self::DependencyCurrent(_) => EvolutionStateFamily::DependencyCurrent,
            Self::TemplateCurrent(_) => EvolutionStateFamily::TemplateCurrent,
            Self::LinkRecord(_) => EvolutionStateFamily::LinkRecord,
            Self::PlanRecord(_) => EvolutionStateFamily::PlanRecord,
            Self::EdgeRecord(_) => EvolutionStateFamily::EdgeRecord,
            Self::RolloutCurrent(_) => EvolutionStateFamily::RolloutCurrent,
            Self::RolloutEvidenceCurrent(_) => EvolutionStateFamily::RolloutEvidenceCurrent,
            Self::RolloutDecision(_) => EvolutionStateFamily::RolloutDecision,
            Self::OccurrenceCurrent(_) => EvolutionStateFamily::OccurrenceCurrent,
            Self::SelectionCurrent(_) => EvolutionStateFamily::SelectionCurrent,
            Self::MigrationRecord(_) => EvolutionStateFamily::MigrationRecord,
            Self::RestartRecord(_) => EvolutionStateFamily::RestartRecord,
            Self::ShadowRecord(_) => EvolutionStateFamily::ShadowRecord,
            Self::ShadowSubjectCurrent(_) => EvolutionStateFamily::ShadowSubjectCurrent,
            Self::ObservationRecord(_) => EvolutionStateFamily::ObservationRecord,
            Self::ObservationOccurrenceCurrent(_) => {
                EvolutionStateFamily::ObservationOccurrenceCurrent
            }
            Self::EvidenceCurrent(_) => EvolutionStateFamily::EvidenceCurrent,
            Self::DecisionTransitionCurrent(_) => EvolutionStateFamily::DecisionTransitionCurrent,
            Self::TransitionRecord(_) => EvolutionStateFamily::TransitionRecord,
        }
    }

    /// Derive the exact content-addressed map key for this mutation.
    ///
    /// # Errors
    ///
    /// Returns an error when an owner or semantic identity cannot form a valid
    /// normalized key.
    pub fn storage_key(&self) -> EvolutionResult<(EvolutionStateFamily, String)> {
        let family = self.family();
        macro_rules! key {
            ($value:expr, $owner:expr, $identity:expr) => {
                evolution_state_key(family, &$value.evolution_id, $owner, $identity)?
            };
        }
        let storage_key = match self {
            Self::DefinitionCurrent(value) => {
                key!(value, &value.logical_ref, "current")
            }
            Self::DependencyCurrent(value) => {
                key!(value, &value.logical_ref, "current")
            }
            Self::DefinitionCompatibilityCurrent(value) => {
                key!(value, &value.logical_ref, &value.contract_id)
            }
            Self::DefinitionRecord(value) => {
                key!(value, &value.logical_ref, &value.latest.revision_id)
            }
            Self::TemplateCurrent(value) => key!(value, &value.template.template_id, "current"),
            Self::LinkRecord(value) => key!(value, &value.template_id, &value.link_id),
            Self::PlanRecord(value) => key!(value, &value.template_id, &value.plan.plan_id),
            Self::EdgeRecord(value) => key!(value, &value.template_id, &value.edge.edge_id),
            Self::RolloutCurrent(value) => key!(value, &value.template_id, "current"),
            Self::RolloutEvidenceCurrent(value) => {
                key!(value, &value.template_id, &value.decision_id)
            }
            Self::RolloutDecision(value) => {
                key!(value, &value.template_id, &value.decision.decision_id)
            }
            Self::OccurrenceCurrent(value) => {
                key!(value, &value.pin.template_id, &value.pin.occurrence_id)
            }
            Self::SelectionCurrent(value) => {
                key!(value, &value.template_id, &value.selection_id)
            }
            Self::MigrationRecord(value) => {
                key!(
                    value,
                    &value.template_id,
                    &value.receipt.request.migration_id
                )
            }
            Self::RestartRecord(value) => {
                key!(value, &value.template_id, &value.receipt.request.restart_id)
            }
            Self::ShadowRecord(value) => {
                key!(value, &value.template_id, &value.comparison.comparison_id)
            }
            Self::ShadowSubjectCurrent(value) => {
                let identity = shadow_subject_identity(&value.decision_id, &value.subject)?;
                key!(value, &value.template_id, &identity)
            }
            Self::ObservationRecord(value) => {
                key!(value, &value.template_id, &value.observation.observation_id)
            }
            Self::ObservationOccurrenceCurrent(value) => {
                key!(value, &value.template_id, &value.occurrence_id)
            }
            Self::EvidenceCurrent(value) => key!(value, &value.template_id, &value.evidence_id),
            Self::DecisionTransitionCurrent(value) => {
                key!(value, &value.template_id, &value.source_decision_id)
            }
            Self::TransitionRecord(value) => {
                key!(value, &value.template_id, &value.transition.transition_id)
            }
        };
        Ok((family, storage_key))
    }

    /// Derive the exact receipt descriptor for this normalized write.
    ///
    /// # Errors
    ///
    /// Returns an error when its storage key or typed value identity cannot be
    /// derived.
    pub fn write(&self) -> EvolutionResult<EvolutionMutationWrite> {
        let (family, storage_key) = self.storage_key()?;
        Ok(EvolutionMutationWrite {
            family,
            storage_key,
            value_id: content_id(EVOLUTION_MUTATION_VALUE_VERSION, self)?,
        })
    }

    /// Verify the closed leaf shape and its fixed canonical byte bound.
    ///
    /// # Errors
    ///
    /// Returns an error when generation, ownership, semantic content, key, or
    /// canonical byte bounds are invalid.
    pub fn verify(&self) -> EvolutionResult<()> {
        verify_mutation_envelope(self)?;
        match self {
            Self::DefinitionCurrent(value) => {
                verify_definition_leaf(value, false)?;
            }
            Self::DefinitionRecord(value) => {
                verify_definition_leaf(value, true)?;
            }
            Self::DefinitionCompatibilityCurrent(value) => {
                verify_definition_compatibility_current(value)?;
            }
            Self::DependencyCurrent(value) => {
                verify_dependency_current(value)?;
            }
            Self::TemplateCurrent(value) => {
                verify_template_current(value)?;
            }
            Self::LinkRecord(value) => {
                verify_link_record(value)?;
            }
            Self::PlanRecord(value) => {
                validate_identity("template", &value.template_id)?;
                value.plan.verify()?;
            }
            Self::EdgeRecord(value) => {
                validate_identity("template", &value.template_id)?;
                super::live_control::verify_plan_edge(&value.edge)?;
                value
                    .evidence
                    .validate()
                    .map_err(|error| EvolutionError::Validation(error.to_string()))?;
            }
            Self::RolloutCurrent(value) => {
                validate_identity("template", &value.template_id)?;
                verify_rollout_decision_shape(&value.decision)?;
            }
            Self::RolloutEvidenceCurrent(value) => {
                validate_identity("template", &value.template_id)?;
                validate_identity("rollout decision", &value.decision_id)?;
                verify_rollout_evidence_current(value)?;
            }
            Self::RolloutDecision(value) => {
                validate_identity("template", &value.template_id)?;
                verify_rollout_decision_shape(&value.decision)?;
            }
            Self::OccurrenceCurrent(value) => {
                super::live_control::verify_occurrence_pin(&value.pin)?;
            }
            Self::SelectionCurrent(value) => {
                verify_selection_current(value)?;
            }
            Self::MigrationRecord(value) => {
                validate_identity("template", &value.template_id)?;
                super::live_control::verify_migration_receipt(&value.receipt)?;
            }
            Self::RestartRecord(value) => {
                validate_identity("template", &value.template_id)?;
                super::live_control::verify_restart_receipt(&value.receipt)?;
            }
            Self::ShadowRecord(value) => {
                validate_identity("template", &value.template_id)?;
                super::live_control::verify_shadow_comparison(&value.comparison)?;
            }
            Self::ShadowSubjectCurrent(value) => {
                validate_identity("template", &value.template_id)?;
                validate_identity("rollout decision", &value.decision_id)?;
                validate_identity("shadow subject", &value.subject)?;
                validate_identity("shadow comparison", &value.comparison_id)?;
            }
            Self::ObservationRecord(value) => {
                validate_identity("template", &value.template_id)?;
                verify_rollout_observation(&value.observation)?;
            }
            Self::ObservationOccurrenceCurrent(value) => {
                validate_identity("template", &value.template_id)?;
                validate_identity("rollout decision", &value.decision_id)?;
                validate_identity("occurrence", &value.occurrence_id)?;
                validate_identity("rollout observation", &value.observation_id)?;
            }
            Self::EvidenceCurrent(value) => {
                validate_identity("template", &value.template_id)?;
                validate_identity("rollout evidence", &value.evidence_id)?;
            }
            Self::DecisionTransitionCurrent(value) => {
                validate_identity("template", &value.template_id)?;
                validate_identity("rollout decision", &value.source_decision_id)?;
                verify_content_id("rollout transition", &value.transition_id)?;
            }
            Self::TransitionRecord(value) => {
                validate_identity("template", &value.template_id)?;
                super::live_control::verify_rollout_transition(&value.transition)?;
            }
        }
        verify_bounded(
            "evolution normalized state leaf",
            self,
            MAX_EVOLUTION_LEAF_BYTES,
        )?;
        let _ = self.storage_key()?;
        Ok(())
    }
}

fn verify_mutation_envelope(mutation: &EvolutionMutation) -> EvolutionResult<()> {
    let (leaf_version, evolution_id) = match mutation {
        EvolutionMutation::DefinitionCurrent(value)
        | EvolutionMutation::DefinitionRecord(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::DefinitionCompatibilityCurrent(value) => {
            (&value.leaf_version, &value.evolution_id)
        }
        EvolutionMutation::DependencyCurrent(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::TemplateCurrent(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::LinkRecord(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::PlanRecord(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::EdgeRecord(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::RolloutCurrent(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::RolloutEvidenceCurrent(value) => {
            (&value.leaf_version, &value.evolution_id)
        }
        EvolutionMutation::RolloutDecision(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::OccurrenceCurrent(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::SelectionCurrent(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::MigrationRecord(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::RestartRecord(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::ShadowRecord(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::ShadowSubjectCurrent(value) => {
            (&value.leaf_version, &value.evolution_id)
        }
        EvolutionMutation::ObservationRecord(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::ObservationOccurrenceCurrent(value) => {
            (&value.leaf_version, &value.evolution_id)
        }
        EvolutionMutation::EvidenceCurrent(value) => (&value.leaf_version, &value.evolution_id),
        EvolutionMutation::DecisionTransitionCurrent(value) => {
            (&value.leaf_version, &value.evolution_id)
        }
        EvolutionMutation::TransitionRecord(value) => (&value.leaf_version, &value.evolution_id),
    };
    if leaf_version != EVOLUTION_STATE_LEAF_VERSION {
        return Err(EvolutionError::Validation(format!(
            "unsupported evolution state leaf version {leaf_version}"
        )));
    }
    validate_identity("evolution authority", evolution_id)?;
    if mutation.revision() == 0 || mutation.revision() > cymule_core::MAX_EXACT_INTEGER {
        return Err(EvolutionError::Validation(
            "evolution state leaf revision is outside the exact range".to_owned(),
        ));
    }
    Ok(())
}

fn verify_definition_leaf(
    value: &EvolutionDefinitionCurrent,
    immutable_record: bool,
) -> EvolutionResult<()> {
    validate_identity("definition reference", &value.logical_ref)?;
    super::live_control::verify_subflow_revision(&value.latest)?;
    if value.latest.logical_ref != value.logical_ref
        || value.max_sequence == 0
        || value.max_sequence > cymule_core::MAX_EXACT_INTEGER
        || value.latest.sequence > value.max_sequence
        || (immutable_record && value.latest.sequence != value.max_sequence)
    {
        return Err(EvolutionError::Validation(
            "definition leaf does not bind a valid lineage sequence and revision".to_owned(),
        ));
    }
    Ok(())
}

fn verify_definition_compatibility_current(
    value: &EvolutionDefinitionCompatibilityCurrent,
) -> EvolutionResult<()> {
    validate_identity("definition reference", &value.logical_ref)?;
    verify_content_id("definition contract", &value.contract_id)?;
    super::live_control::verify_subflow_revision(&value.latest)?;
    if value.latest.logical_ref != value.logical_ref
        || definition_contract_id(
            &value.latest.definition.input_schema,
            &value.latest.definition.output_schema,
        )? != value.contract_id
    {
        return Err(EvolutionError::Validation(
            "definition compatibility current does not match its revision contract".to_owned(),
        ));
    }
    Ok(())
}

fn verify_dependency_current(value: &EvolutionDependencyCurrent) -> EvolutionResult<()> {
    validate_identity("definition reference", &value.logical_ref)?;
    if value.template_ids.len() > MAX_EVOLUTION_PUBLICATION_TEMPLATES {
        return Err(EvolutionError::Validation(
            "definition dependency leaf exceeds the template bound".to_owned(),
        ));
    }
    for template_id in &value.template_ids {
        validate_identity("template", template_id)?;
    }
    Ok(())
}

fn verify_template_current(value: &EvolutionTemplateCurrent) -> EvolutionResult<()> {
    super::linker::validate_template_shape(&value.template)?;
    super::live_control::verify_linked_plan(&value.linked)?;
    if value.template.template_id != value.linked.template_id {
        return Err(EvolutionError::Validation(
            "template current does not match its linked Plan".to_owned(),
        ));
    }
    Ok(())
}

fn verify_link_record(value: &EvolutionLinkRecord) -> EvolutionResult<()> {
    validate_identity("template", &value.template_id)?;
    verify_content_id("linked Plan record", &value.link_id)?;
    super::live_control::verify_linked_plan(&value.linked)?;
    if value.template_id != value.linked.template_id
        || value.link_id != linked_plan_record_id(&value.linked)?
    {
        return Err(EvolutionError::Validation(
            "link record does not match its exact linked Plan".to_owned(),
        ));
    }
    Ok(())
}

fn verify_selection_current(value: &EvolutionSelectionCurrent) -> EvolutionResult<()> {
    validate_identity("template", &value.template_id)?;
    validate_identity("occurrence selection", &value.selection_id)?;
    validate_identity("occurrence", &value.occurrence_id)?;
    validate_identity("rollout decision", &value.decision_id)?;
    verify_content_id("selected Plan", &value.plan_id)?;
    verify_execution_binding_ref(&value.execution_binding)
}

fn verify_rollout_decision_shape(decision: &RolloutDecision) -> EvolutionResult<()> {
    super::live_control::verify_rollout_decision(decision)
}

fn verify_rollout_evidence_current(
    evidence: &EvolutionRolloutEvidenceCurrent,
) -> EvolutionResult<()> {
    for value in [
        evidence.target_observations,
        evidence.target_failures,
        evidence.equivalent_shadows,
        evidence.inequivalent_shadows,
        evidence.evidence_count,
    ] {
        if value > cymule_core::MAX_EXACT_INTEGER {
            return Err(EvolutionError::Validation(
                "rollout evidence aggregate exceeds the exact range".to_owned(),
            ));
        }
    }
    if evidence.target_failures > evidence.target_observations {
        return Err(EvolutionError::Validation(
            "rollout failures exceed target observations".to_owned(),
        ));
    }
    let expected_count = evidence
        .target_observations
        .checked_add(evidence.equivalent_shadows)
        .and_then(|count| count.checked_add(evidence.inequivalent_shadows))
        .ok_or_else(|| {
            EvolutionError::Validation("rollout evidence count overflowed".to_owned())
        })?;
    if evidence.evidence_count != expected_count {
        return Err(EvolutionError::Validation(
            "rollout evidence count does not match its typed aggregates".to_owned(),
        ));
    }
    verify_content_id("rollout evidence root", &evidence.evidence_root)
}

fn verify_rollout_observation(observation: &RolloutObservation) -> EvolutionResult<()> {
    super::live_control::verify_rollout_observation(observation)
}

/// Derive an exact persistent-map key without delimiter ambiguity.
///
/// # Errors
///
/// Returns an error when an authority, owner, or semantic identity is invalid.
pub fn evolution_state_key(
    family: EvolutionStateFamily,
    evolution_id: &str,
    owner: &str,
    identity: &str,
) -> EvolutionResult<String> {
    validate_identity("evolution authority", evolution_id)?;
    validate_identity("evolution state owner", owner)?;
    validate_identity("evolution state identity", identity)?;
    content_id(
        EVOLUTION_STATE_KEY_VERSION,
        &(family, evolution_id, owner, identity),
    )
    .map_err(Into::into)
}

/// Derive the exact scalar-current key for one M4 authority partition.
///
/// # Errors
///
/// Returns an error when the authority identity is invalid.
pub fn evolution_current_key(evolution_id: &str) -> EvolutionResult<String> {
    validate_identity("evolution authority", evolution_id)?;
    content_id(EVOLUTION_CURRENT_KEY_VERSION, &evolution_id).map_err(Into::into)
}

/// Derive the exact all-ever command-alias key for one M4 partition.
///
/// # Errors
///
/// Returns an error when the authority or command identity is invalid.
pub fn evolution_command_alias_key(
    evolution_id: &str,
    command_id: &str,
) -> EvolutionResult<String> {
    validate_identity("evolution authority", evolution_id)?;
    validate_identity("live-evolution command", command_id)?;
    content_id(
        EVOLUTION_COMMAND_ALIAS_KEY_VERSION,
        &(evolution_id, command_id),
    )
    .map_err(Into::into)
}

/// Derive the exact all-ever semantic-receipt key for one M4 partition.
///
/// # Errors
///
/// Returns an error when the authority or receipt identity is invalid.
pub fn evolution_receipt_key(evolution_id: &str, receipt_id: &str) -> EvolutionResult<String> {
    validate_identity("evolution authority", evolution_id)?;
    verify_content_id("evolution persistence receipt", receipt_id)?;
    content_id(EVOLUTION_RECEIPT_KEY_VERSION, &(evolution_id, receipt_id)).map_err(Into::into)
}

fn shadow_subject_identity(decision_id: &str, subject: &str) -> EvolutionResult<String> {
    validate_identity("rollout decision", decision_id)?;
    validate_identity("shadow subject", subject)?;
    content_id(SHADOW_SUBJECT_ID_DOMAIN, &(decision_id, subject)).map_err(Into::into)
}

fn definition_contract_id(
    input_schema: &serde_json::Value,
    output_schema: &serde_json::Value,
) -> EvolutionResult<String> {
    content_id(
        DEFINITION_CONTRACT_ID_DOMAIN,
        &(input_schema, output_schema),
    )
    .map_err(Into::into)
}

fn linked_plan_record_id(linked: &LinkedPlan) -> EvolutionResult<String> {
    content_id(LINK_RECORD_ID_DOMAIN, linked).map_err(Into::into)
}

/// Derive the canonical empty root for one decision's evidence accumulator.
///
/// # Errors
///
/// Returns an error when canonical identity derivation fails.
pub fn empty_evolution_evidence_root() -> EvolutionResult<String> {
    content_id(EVOLUTION_EVIDENCE_ROOT_VERSION, &()).map_err(Into::into)
}

/// Advance the authenticated evidence accumulator by one exact record ID.
///
/// # Errors
///
/// Returns an error when either identity is malformed or the new root cannot
/// be derived.
pub fn advance_evolution_evidence_root(
    parent_root: &str,
    evidence_id: &str,
) -> EvolutionResult<String> {
    verify_content_id("rollout evidence parent root", parent_root)?;
    validate_identity("rollout evidence", evidence_id)?;
    content_id(EVOLUTION_EVIDENCE_ROOT_VERSION, &(parent_root, evidence_id)).map_err(Into::into)
}

pub(crate) fn verify_bounded<T: Serialize + ?Sized>(
    kind: &str,
    value: &T,
    maximum: usize,
) -> EvolutionResult<()> {
    let bytes = canonical_bytes(&value)?;
    if bytes.len() > maximum {
        return Err(EvolutionError::Validation(format!(
            "{kind} uses {} canonical bytes, above the {maximum} byte bound",
            bytes.len()
        )));
    }
    Ok(())
}

fn verify_content_id(kind: &str, value: &str) -> EvolutionResult<()> {
    cymule_core::validate_content_id(kind, value).map_err(Into::into)
}

fn exact_leaf<'a>(
    view: &'a EvolutionAuthorityView,
    family: EvolutionStateFamily,
    evolution_id: &str,
    owner: &str,
    identity: &str,
) -> EvolutionResult<Option<&'a EvolutionMutation>> {
    let key = evolution_state_key(family, evolution_id, owner, identity)?;
    view.lookup(family, key)
}

macro_rules! typed_leaf {
    ($name:ident, $family:ident, $variant:ident, $type:ty) => {
        fn $name<'a>(
            view: &'a EvolutionAuthorityView,
            evolution_id: &str,
            owner: &str,
            identity: &str,
        ) -> EvolutionResult<Option<&'a $type>> {
            match exact_leaf(
                view,
                EvolutionStateFamily::$family,
                evolution_id,
                owner,
                identity,
            )? {
                Some(EvolutionMutation::$variant(value)) => Ok(Some(value)),
                Some(_) => Err(EvolutionError::Conflict(
                    "evolution StateRoot family contains the wrong closed leaf variant".to_owned(),
                )),
                None => Ok(None),
            }
        }
    };
}

typed_leaf!(
    definition_current,
    DefinitionCurrent,
    DefinitionCurrent,
    EvolutionDefinitionCurrent
);
typed_leaf!(
    definition_compatibility_current,
    DefinitionCompatibilityCurrent,
    DefinitionCompatibilityCurrent,
    EvolutionDefinitionCompatibilityCurrent
);
typed_leaf!(
    definition_record,
    DefinitionRecord,
    DefinitionRecord,
    EvolutionDefinitionCurrent
);
typed_leaf!(
    dependency_current,
    DependencyCurrent,
    DependencyCurrent,
    EvolutionDependencyCurrent
);
typed_leaf!(
    template_current,
    TemplateCurrent,
    TemplateCurrent,
    EvolutionTemplateCurrent
);
typed_leaf!(link_record, LinkRecord, LinkRecord, EvolutionLinkRecord);
typed_leaf!(plan_record, PlanRecord, PlanRecord, EvolutionPlanRecord);
typed_leaf!(edge_record, EdgeRecord, EdgeRecord, EvolutionEdgeRecord);
typed_leaf!(
    rollout_current,
    RolloutCurrent,
    RolloutCurrent,
    EvolutionRolloutCurrent
);
typed_leaf!(
    rollout_evidence_current,
    RolloutEvidenceCurrent,
    RolloutEvidenceCurrent,
    EvolutionRolloutEvidenceCurrent
);
typed_leaf!(
    rollout_decision_record,
    RolloutDecision,
    RolloutDecision,
    EvolutionRolloutDecisionRecord
);
typed_leaf!(
    occurrence_current,
    OccurrenceCurrent,
    OccurrenceCurrent,
    EvolutionOccurrenceCurrent
);
typed_leaf!(
    selection_current,
    SelectionCurrent,
    SelectionCurrent,
    EvolutionSelectionCurrent
);
typed_leaf!(
    migration_record,
    MigrationRecord,
    MigrationRecord,
    EvolutionMigrationRecord
);
typed_leaf!(
    restart_record,
    RestartRecord,
    RestartRecord,
    EvolutionRestartRecord
);
typed_leaf!(
    shadow_record,
    ShadowRecord,
    ShadowRecord,
    EvolutionShadowRecord
);
typed_leaf!(
    shadow_subject_current,
    ShadowSubjectCurrent,
    ShadowSubjectCurrent,
    EvolutionShadowSubjectCurrent
);
typed_leaf!(
    observation_record,
    ObservationRecord,
    ObservationRecord,
    EvolutionObservationRecord
);
typed_leaf!(
    observation_occurrence_current,
    ObservationOccurrenceCurrent,
    ObservationOccurrenceCurrent,
    EvolutionObservationOccurrenceCurrent
);
typed_leaf!(
    evidence_current,
    EvidenceCurrent,
    EvidenceCurrent,
    EvolutionEvidenceCurrent
);
typed_leaf!(
    decision_transition_current,
    DecisionTransitionCurrent,
    DecisionTransitionCurrent,
    EvolutionDecisionTransitionCurrent
);
typed_leaf!(
    transition_record,
    TransitionRecord,
    TransitionRecord,
    EvolutionTransitionRecord
);

fn prevalidate_command(
    view: &EvolutionAuthorityView,
    persistence: &EvolutionPersistenceCommand,
    source: &EvolutionReductionSourceBody,
) -> EvolutionResult<()> {
    match &persistence.command {
        LiveEvolutionCommand::Apply {
            template_id,
            command,
            ..
        } => prevalidate_apply_command(
            view,
            &persistence.evolution_id,
            template_id,
            command,
            source,
        ),
        _ => require_no_runtime_source(source),
    }
}

fn prevalidate_apply_command(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    template_id: &str,
    command: &EvolutionCommand,
    source: &EvolutionReductionSourceBody,
) -> EvolutionResult<()> {
    if template_current(view, evolution_id, template_id, "current")?.is_none() {
        return Err(EvolutionError::NotFound(format!(
            "live evolution template {template_id} is missing"
        )));
    }
    match command {
        EvolutionCommand::SelectOccurrence {
            occurrence_id,
            selection_id,
            execution_binding,
            ..
        } => prevalidate_selection(
            view,
            evolution_id,
            template_id,
            occurrence_id,
            selection_id,
            execution_binding,
            source,
        ),
        EvolutionCommand::Migrate { request, .. } => {
            prevalidate_migration_source(view, evolution_id, template_id, request, source)
        }
        EvolutionCommand::RestartUnderNewPlan { request, .. } => {
            let EvolutionReductionSourceBody::Restart {
                safe_point,
                continuation,
            } = source
            else {
                return Err(EvolutionError::Validation(
                    "restart command requires Durable-derived restart authority".to_owned(),
                ));
            };
            validate_restart_preflight(request, safe_point, continuation)
        }
        EvolutionCommand::Shadow { request, .. } => {
            prevalidate_shadow(view, evolution_id, template_id, request, source)
        }
        _ => require_no_runtime_source(source),
    }
}

fn prevalidate_selection(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    template_id: &str,
    occurrence_id: &str,
    selection_id: &str,
    requested_binding: &ArtifactRef,
    source: &EvolutionReductionSourceBody,
) -> EvolutionResult<()> {
    let EvolutionReductionSourceBody::Selection {
        plan_id,
        execution_binding,
    } = source
    else {
        return Err(EvolutionError::Validation(
            "occurrence selection requires Durable-derived binding authority".to_owned(),
        ));
    };
    let lineage =
        occurrence_selection_lineage(view, evolution_id, template_id, occurrence_id, selection_id)?;
    if lineage.plan.plan_id != *plan_id {
        return Err(EvolutionError::Conflict(
            "occurrence binding authority targets a different selected Plan".to_owned(),
        ));
    }
    if execution_binding.reference != *requested_binding {
        return Err(EvolutionError::Conflict(
            "loaded ExecutionBinding record does not match the semantic selection".to_owned(),
        ));
    }
    if lineage
        .existing_pin
        .as_ref()
        .is_some_and(|pin| pin.execution_binding != execution_binding.reference)
    {
        return Err(EvolutionError::Conflict(
            "retained occurrence has a different execution binding".to_owned(),
        ));
    }
    Ok(())
}

fn prevalidate_shadow(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    template_id: &str,
    request: &super::ShadowRequest,
    source: &EvolutionReductionSourceBody,
) -> EvolutionResult<()> {
    require_no_runtime_source(source)?;
    let decision = rollout_decision_record(view, evolution_id, template_id, &request.decision_id)?
        .ok_or_else(|| EvolutionError::NotFound("shadow rollout decision is missing".to_owned()))?;
    if decision.decision.fallback_plan != request.primary_plan
        || decision.decision.target_plan != request.shadow_plan
    {
        return Err(EvolutionError::Conflict(
            "shadow request does not match the exact rollout current".to_owned(),
        ));
    }
    request.verify()?;
    let _ = shadow_record(view, evolution_id, template_id, &request.comparison_id)?;
    let subject_identity = shadow_subject_identity(&request.decision_id, &request.subject)?;
    let _ = shadow_subject_current(view, evolution_id, template_id, &subject_identity)?;
    let _ = evidence_current(view, evolution_id, template_id, &request.comparison_id)?;
    rollout_evidence_current(view, evolution_id, template_id, &request.decision_id)?
        .ok_or_else(|| EvolutionError::NotFound("shadow rollout evidence is missing".to_owned()))?;
    Ok(())
}

fn command_requires_provider(
    view: &EvolutionAuthorityView,
    persistence: &EvolutionPersistenceCommand,
) -> EvolutionResult<bool> {
    let LiveEvolutionCommand::Apply {
        template_id,
        command,
        ..
    } = &persistence.command
    else {
        return Ok(false);
    };
    match command.as_ref() {
        EvolutionCommand::Migrate { request, .. } => Ok(migration_record(
            view,
            &persistence.evolution_id,
            template_id,
            &request.migration_id,
        )?
        .is_none()),
        EvolutionCommand::Shadow { request, .. } => Ok(shadow_record(
            view,
            &persistence.evolution_id,
            template_id,
            &request.comparison_id,
        )?
        .is_none()),
        _ => Ok(false),
    }
}

fn require_no_runtime_source(source: &EvolutionReductionSourceBody) -> EvolutionResult<()> {
    match source {
        EvolutionReductionSourceBody::None => Ok(()),
        _ => Err(EvolutionError::Conflict(
            "Durable runtime authority was supplied to a command that does not consume it"
                .to_owned(),
        )),
    }
}

fn retained_migration_record<'a>(
    view: &'a EvolutionAuthorityView,
    command: &EvolutionPersistenceCommand,
) -> EvolutionResult<&'a EvolutionMigrationRecord> {
    let LiveEvolutionCommand::Apply {
        template_id,
        command: inner,
        ..
    } = &command.command
    else {
        return Err(EvolutionError::Validation(
            "retained migration replay requires one Apply command".to_owned(),
        ));
    };
    let EvolutionCommand::Migrate { request, .. } = inner.as_ref() else {
        return Err(EvolutionError::Validation(
            "retained migration replay requires Migrate".to_owned(),
        ));
    };
    let retained = migration_record(
        view,
        &command.evolution_id,
        template_id,
        &request.migration_id,
    )?
    .ok_or_else(|| EvolutionError::NotFound("retained migration record is missing".to_owned()))?;
    if retained.receipt.request != **request {
        return Err(EvolutionError::Conflict(
            "retained migration request differs from its semantic command".to_owned(),
        ));
    }
    Ok(retained)
}

fn prevalidate_migration_source(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    template_id: &str,
    request: &super::MigrationRequest,
    source: &EvolutionReductionSourceBody,
) -> EvolutionResult<()> {
    let (target_plan, retained) =
        migration_plan_preflight(view, evolution_id, template_id, request)?;
    match (retained, source) {
        (Some(record), EvolutionReductionSourceBody::RetainedMigration { target_binding }) => {
            if record.receipt.target_binding != target_binding.reference {
                return Err(EvolutionError::Conflict(
                    "retained migration target binding differs from its exact record".to_owned(),
                ));
            }
            verify_evolution_target_binding_record(target_plan, target_binding)
        }
        (Some(_), _) => Err(EvolutionError::Conflict(
            "retained migration replay received fresh or unrelated runtime authority".to_owned(),
        )),
        (None, EvolutionReductionSourceBody::Migration { .. }) => {
            validate_migration_preflight(view, evolution_id, template_id, request, source)
        }
        (None, EvolutionReductionSourceBody::RetainedMigration { .. }) => {
            Err(EvolutionError::NotFound(
                "retained migration replay has no exact migration record".to_owned(),
            ))
        }
        (None, _) => Err(EvolutionError::Validation(
            "fresh migration requires Durable-derived migration authority".to_owned(),
        )),
    }
}

fn validate_migration_preflight(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    template_id: &str,
    request: &super::MigrationRequest,
    source: &EvolutionReductionSourceBody,
) -> EvolutionResult<()> {
    let EvolutionReductionSourceBody::Migration {
        safe_point,
        continuation,
        source_binding,
        target_binding,
    } = source
    else {
        return Err(EvolutionError::Validation(
            "migration command requires Durable-derived migration authority".to_owned(),
        ));
    };
    safe_point.verify()?;
    safe_point.verify_source_continuation(continuation)?;
    verify_execution_binding_ref(source_binding)?;
    if request.run_id != safe_point.run_id
        || request.from_plan != safe_point.plan_id
        || request.expected_source_epoch != safe_point.epoch
        || continuation.binding_context != source_binding.artifact_id
    {
        return Err(EvolutionError::Conflict(
            "migration request does not match its exact safe point".to_owned(),
        ));
    }
    let (target_plan, _) = migration_plan_preflight(view, evolution_id, template_id, request)?;
    verify_evolution_target_binding_record(target_plan, target_binding)?;
    Ok(())
}

fn migration_plan_preflight<'a>(
    view: &'a EvolutionAuthorityView,
    evolution_id: &str,
    template_id: &str,
    request: &super::MigrationRequest,
) -> EvolutionResult<(&'a SealedPlan, Option<&'a EvolutionMigrationRecord>)> {
    let source = plan_record(view, evolution_id, template_id, &request.from_plan)?
        .ok_or_else(|| EvolutionError::NotFound("migration source Plan is missing".to_owned()))?;
    let target = plan_record(view, evolution_id, template_id, &request.to_plan)?
        .ok_or_else(|| EvolutionError::NotFound("migration target Plan is missing".to_owned()))?;
    source.plan.verify()?;
    target.plan.verify()?;
    let edge = edge_record(view, evolution_id, template_id, &request.plan_edge_id)?
        .ok_or_else(|| EvolutionError::NotFound("migration Plan edge is missing".to_owned()))?;
    if edge.edge.from_plan != request.from_plan || edge.edge.to_plan != request.to_plan {
        return Err(EvolutionError::Conflict(
            "migration Plan edge does not match the requested transition".to_owned(),
        ));
    }
    let compatibility = analyze_relink(&source.plan, &target.plan)?;
    if compatibility.compatibility_id != request.compatibility_id {
        return Err(EvolutionError::Conflict(
            "migration request does not match deterministic compatibility".to_owned(),
        ));
    }
    super::compatibility::validate_migration_no_widening(&source.plan, &target.plan)?;
    let retained = migration_record(view, evolution_id, template_id, &request.migration_id)?;
    if retained.is_some_and(|record| record.receipt.request != *request) {
        return Err(EvolutionError::Conflict(
            "migration identity was reused with different semantic intent".to_owned(),
        ));
    }
    Ok((&target.plan, retained))
}

fn migration_adapter_request(
    intent: &super::MigrationRequest,
    safe_point: &MigrationSafePoint,
    continuation: &cymule_durable_protocol::Continuation,
    source_binding: &ArtifactRef,
    target_binding: &ArtifactRecord,
) -> EvolutionResult<MigrationAdapterRequest> {
    let input_state = safe_point.state.clone().ok_or_else(|| {
        EvolutionError::Conflict("migration source witness has no state Artifact".to_owned())
    })?;
    Ok(MigrationAdapterRequest {
        intent: intent.clone(),
        source_witness_id: safe_point.safe_point_id.clone(),
        source_continuation: continuation.clone(),
        input_state,
        source_binding: source_binding.clone(),
        target_binding: target_binding.reference.clone(),
    })
}

fn validate_restart_preflight(
    request: &super::RestartRequest,
    safe_point: &MigrationSafePoint,
    continuation: &cymule_durable_protocol::Continuation,
) -> EvolutionResult<()> {
    super::live_control::verify_restart_request(request)?;
    safe_point.verify()?;
    safe_point.verify_source_continuation(continuation)?;
    if request.run_id != safe_point.run_id
        || request.from_plan != safe_point.plan_id
        || request.expected_source_epoch != safe_point.epoch
    {
        return Err(EvolutionError::Conflict(
            "restart intent does not match the exact Durable source witness".to_owned(),
        ));
    }
    Ok(())
}

fn reduce_command(
    view: &EvolutionAuthorityView,
    persistence: &EvolutionPersistenceCommand,
    revision: u64,
    source: &EvolutionReductionSourceBody,
    provider: EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    match &persistence.command {
        LiveEvolutionCommand::PublishDefinition {
            logical_ref,
            definition,
            references,
            ..
        } => reduce_definition_publication(
            view,
            persistence,
            revision,
            logical_ref,
            definition,
            references,
            &provider,
        ),
        LiveEvolutionCommand::RegisterTemplate { template, .. } => {
            reduce_template_registration(view, persistence, revision, template, &provider)
        }
        LiveEvolutionCommand::PublishAndRelink { publication, .. } => {
            reduce_publish_and_relink(view, persistence, revision, publication, &provider)
        }
        LiveEvolutionCommand::Apply {
            template_id,
            command,
            ..
        } => reduce_template_command(
            view,
            persistence,
            revision,
            template_id,
            command,
            source,
            provider,
        ),
    }
}

fn finish_postcondition(
    command: EvolutionPersistenceCommand,
    revision: u64,
    parent_current_id: Option<String>,
    source_witness_id: Option<String>,
    mut reduced: ReducedEvolution,
) -> EvolutionResult<EvolutionPostcondition> {
    let mut keyed_mutations = reduced
        .mutations
        .into_iter()
        .map(|mutation| Ok((mutation.storage_key()?, mutation)))
        .collect::<EvolutionResult<Vec<_>>>()?;
    keyed_mutations.sort_by(|left, right| left.0.cmp(&right.0));
    reduced.mutations = keyed_mutations
        .into_iter()
        .map(|(_, mutation)| mutation)
        .collect();
    let mut mutation_keys = BTreeSet::new();
    for mutation in &reduced.mutations {
        mutation.verify()?;
        if !mutation_keys.insert(mutation.storage_key()?) {
            return Err(EvolutionError::Validation(
                "evolution postcondition repeats one normalized state key".to_owned(),
            ));
        }
    }
    if reduced.mutations.len() > MAX_EVOLUTION_TRANSITION_LEAVES {
        return Err(EvolutionError::Validation(
            "evolution postcondition exceeds the normalized mutation bound".to_owned(),
        ));
    }
    reduced
        .plans
        .sort_by(|left, right| left.plan_id.cmp(&right.plan_id));
    reduced.plans.dedup_by(|left, right| left == right);
    for plan in &reduced.plans {
        plan.verify()?;
    }
    reduced
        .artifacts
        .sort_by(|left, right| left.reference.cmp(&right.reference));
    reduced.artifacts.dedup_by(|left, right| left == right);
    for artifact in &reduced.artifacts {
        verify_artifact_record(artifact)?;
        reduced.required_artifacts.remove(&artifact.reference);
    }
    for reference in &reduced.required_artifacts {
        reference
            .validate()
            .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    }
    verify_postcondition_aggregate(
        None,
        None,
        None,
        &reduced.mutations,
        &reduced.plans,
        &reduced.artifacts,
        &reduced.required_artifacts,
    )?;
    reduced.outcome.verify_wire()?;
    super::live_control::verify_command_outcome(&command.command, &reduced.outcome)?;
    let mutation_writes = reduced
        .mutations
        .iter()
        .map(EvolutionMutation::write)
        .collect::<EvolutionResult<Vec<_>>>()?;
    let mutation_id = content_id(EVOLUTION_MUTATION_SET_VERSION, &mutation_writes)?;
    let mut receipt = EvolutionPersistenceReceipt {
        receipt_version: EVOLUTION_PERSISTENCE_RECEIPT_VERSION.to_owned(),
        receipt_id: String::new(),
        command,
        parent_current_id,
        source_witness_id,
        outcome: reduced.outcome,
        mutations: mutation_writes,
        mutation_id,
    };
    receipt.receipt_id = receipt.derived_id()?;
    receipt.verify()?;
    let mut current = EvolutionCurrent {
        current_version: EVOLUTION_CURRENT_VERSION.to_owned(),
        current_id: String::new(),
        evolution_id: receipt.command.evolution_id.clone(),
        revision,
        last_receipt_id: receipt.receipt_id.clone(),
    };
    current.current_id = current.derived_id()?;
    current.verify()?;
    let postcondition = EvolutionPostcondition {
        alias: EvolutionCommandAlias {
            evolution_id: receipt.command.evolution_id.clone(),
            command_id: receipt.command.command.command_id().to_owned(),
            persistence_id: receipt.command.persistence_id.clone(),
            receipt_id: receipt.receipt_id.clone(),
        },
        current,
        receipt,
        mutations: reduced.mutations,
        plans: reduced.plans,
        artifacts: reduced.artifacts,
        required_artifacts: reduced.required_artifacts,
    };
    postcondition.verify()?;
    Ok(postcondition)
}

impl EvolutionCurrent {
    fn derived_id(&self) -> EvolutionResult<String> {
        content_id(
            EVOLUTION_CURRENT_VERSION,
            &(
                self.current_version.as_str(),
                self.evolution_id.as_str(),
                self.revision,
                self.last_receipt_id.as_str(),
            ),
        )
        .map_err(Into::into)
    }
}

impl EvolutionPersistenceReceipt {
    fn derived_id(&self) -> EvolutionResult<String> {
        content_id(
            EVOLUTION_PERSISTENCE_RECEIPT_VERSION,
            &(
                self.receipt_version.as_str(),
                &self.command,
                &self.parent_current_id,
                &self.source_witness_id,
                &self.outcome,
                &self.mutations,
                self.mutation_id.as_str(),
            ),
        )
        .map_err(Into::into)
    }

    /// Verify the semantic receipt without a physical manifest or CAS token.
    ///
    /// # Errors
    ///
    /// Returns an error when the command, source binding, outcome, ordered
    /// writes, identities, or canonical byte bound is invalid.
    pub fn verify(&self) -> EvolutionResult<()> {
        if self.receipt_version != EVOLUTION_PERSISTENCE_RECEIPT_VERSION {
            return Err(EvolutionError::Validation(
                "unsupported evolution persistence receipt version".to_owned(),
            ));
        }
        self.command.verify()?;
        if let Some(parent) = &self.parent_current_id {
            verify_content_id("parent evolution current", parent)?;
        }
        if let Some(source_witness) = &self.source_witness_id {
            verify_content_id("evolution source witness", source_witness)?;
        }
        let consumes_source = matches!(
            &self.command.command,
            LiveEvolutionCommand::Apply { command, .. }
                if matches!(
                    command.as_ref(),
                    EvolutionCommand::Migrate { .. }
                        | EvolutionCommand::RestartUnderNewPlan { .. }
                )
        );
        if consumes_source != self.source_witness_id.is_some() {
            return Err(EvolutionError::Validation(
                "evolution receipt source witness does not match its semantic command".to_owned(),
            ));
        }
        let outcome_source_witness = match &self.outcome {
            LiveEvolutionOutcome::Migrated { receipt } => Some(receipt.source_witness_id.as_str()),
            LiveEvolutionOutcome::RestartAuthorized { receipt } => {
                Some(receipt.source_witness_id.as_str())
            }
            _ => None,
        };
        if self.source_witness_id.as_deref() != outcome_source_witness {
            return Err(EvolutionError::Validation(
                "evolution receipt source witness differs from its semantic outcome".to_owned(),
            ));
        }
        self.outcome.verify_wire()?;
        super::live_control::verify_command_outcome(&self.command.command, &self.outcome)?;
        if self.mutations.len() > MAX_EVOLUTION_TRANSITION_LEAVES {
            return Err(EvolutionError::Validation(
                "evolution receipt exceeds the normalized mutation bound".to_owned(),
            ));
        }
        let mut previous_key = None;
        for mutation in &self.mutations {
            mutation.verify()?;
            let key = (mutation.family, mutation.storage_key.as_str());
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(EvolutionError::Validation(
                    "evolution receipt mutations are not strictly key-ordered".to_owned(),
                ));
            }
            previous_key = Some(key);
        }
        if self.mutation_id != content_id(EVOLUTION_MUTATION_SET_VERSION, &self.mutations)? {
            return Err(EvolutionError::Validation(
                "evolution mutation-set identity does not match its exact writes".to_owned(),
            ));
        }
        if self.receipt_id != self.derived_id()? {
            return Err(EvolutionError::Validation(
                "evolution receipt identity does not match its semantic body".to_owned(),
            ));
        }
        verify_bounded(
            "evolution persistence receipt",
            self,
            MAX_EVOLUTION_RECEIPT_BYTES,
        )
    }
}

impl EvolutionCommandAlias {
    /// Verify one exact command-to-receipt alias.
    ///
    /// # Errors
    ///
    /// Returns an error when any exact alias identity is malformed.
    pub fn verify(&self) -> EvolutionResult<()> {
        validate_identity("evolution authority", &self.evolution_id)?;
        validate_identity("live-evolution command", &self.command_id)?;
        verify_content_id("evolution persistence command", &self.persistence_id)?;
        verify_content_id("evolution persistence receipt", &self.receipt_id)
    }
}

impl EvolutionMutationWrite {
    /// Verify the exact normalized key and value identities retained by a
    /// semantic receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the key or value identity is malformed.
    pub fn verify(&self) -> EvolutionResult<()> {
        verify_content_id("evolution mutation storage key", &self.storage_key)?;
        verify_content_id("evolution mutation value", &self.value_id)
    }
}

impl EvolutionPostcondition {
    /// Verify all cross-object bindings before Durable lowers the mutations.
    ///
    /// # Errors
    ///
    /// Returns an error when the current, alias, receipt, mutation set, Plan or
    /// Artifact closure, ordering, ownership, or aggregate bounds differ.
    pub fn verify(&self) -> EvolutionResult<()> {
        self.current.verify()?;
        self.alias.verify()?;
        self.receipt.verify()?;
        verify_postcondition_binding(&self.current, &self.alias, &self.receipt)?;
        let mutation_writes = self
            .mutations
            .iter()
            .map(EvolutionMutation::write)
            .collect::<EvolutionResult<Vec<_>>>()?;
        if mutation_writes != self.receipt.mutations
            || content_id(EVOLUTION_MUTATION_SET_VERSION, &mutation_writes)?
                != self.receipt.mutation_id
        {
            return Err(EvolutionError::Validation(
                "evolution postcondition exact writes do not match its receipt".to_owned(),
            ));
        }
        verify_postcondition_mutations(&self.current, &self.mutations)?;
        verify_edge_mutation_authority(
            &self.receipt.command.command,
            &self.receipt.outcome,
            &self.mutations,
            &self.artifacts,
            &self.required_artifacts,
        )?;
        verify_migration_material_authority(
            &self.receipt.command.command,
            &self.receipt.outcome,
            &self.mutations,
            &self.artifacts,
            &self.required_artifacts,
        )?;
        verify_postcondition_plans(&self.plans)?;
        verify_postcondition_artifacts(&self.artifacts, &self.required_artifacts)?;
        verify_postcondition_aggregate(
            Some(&self.current),
            Some(&self.alias),
            Some(&self.receipt),
            &self.mutations,
            &self.plans,
            &self.artifacts,
            &self.required_artifacts,
        )?;
        Ok(())
    }

    /// Project the exact Core migration command and target Continuation for one
    /// fresh migration. Exact semantic-record replay returns `None` because its
    /// M1 migration already committed.
    ///
    /// # Errors
    ///
    /// Returns an error when the postcondition is invalid or a migration
    /// command and outcome no longer form one exact typed sidecar.
    pub fn migration_sidecar(&self) -> EvolutionResult<Option<EvolutionMigrationSidecar>> {
        self.verify()?;
        let LiveEvolutionCommand::Apply { command, .. } = &self.receipt.command.command else {
            return Ok(None);
        };
        let EvolutionCommand::Migrate { request, .. } = command.as_ref() else {
            return Ok(None);
        };
        let LiveEvolutionOutcome::Migrated { receipt } = &self.receipt.outcome else {
            return Err(EvolutionError::Validation(
                "migration postcondition has no typed migration outcome".to_owned(),
            ));
        };
        if receipt.request != **request {
            return Err(EvolutionError::Validation(
                "migration sidecar request differs from its semantic command".to_owned(),
            ));
        }
        if !self
            .mutations
            .iter()
            .any(|mutation| matches!(mutation, EvolutionMutation::MigrationRecord(_)))
        {
            return Ok(None);
        }
        let target_continuation_digest =
            cymule_core::canonical_digest(&receipt.target_continuation)?;
        Ok(Some(EvolutionMigrationSidecar {
            command_id: self.receipt.command.persistence_id.clone(),
            run_id: receipt.request.run_id.clone(),
            command: cymule_core::Command::MigrateRun {
                from_plan: receipt.request.from_plan.clone(),
                to_plan: receipt.request.to_plan.clone(),
                from_binding: receipt.source_binding.artifact_id.clone(),
                to_binding: receipt.target_binding.artifact_id.clone(),
                safe_point_id: receipt.source_witness_id.clone(),
                target_epoch: receipt.target_epoch,
                target_continuation_digest,
            },
            target_continuation: receipt.target_continuation.clone(),
        }))
    }
}

fn verify_edge_mutation_authority(
    command: &LiveEvolutionCommand,
    outcome: &LiveEvolutionOutcome,
    mutations: &[EvolutionMutation],
    artifacts: &[ArtifactRecord],
    required_artifacts: &BTreeSet<ArtifactRef>,
) -> EvolutionResult<()> {
    let edges = mutations
        .iter()
        .filter_map(|mutation| match mutation {
            EvolutionMutation::EdgeRecord(record) => Some(record.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
    match command {
        LiveEvolutionCommand::Apply { command, .. } => match command.as_ref() {
            EvolutionCommand::ApplyPatch { patch, .. } => {
                if edges.len() != 1
                    || edges[0].evidence != patch.evidence
                    || !artifacts.is_empty()
                    || required_artifacts != &BTreeSet::from([patch.evidence.clone()])
                {
                    return Err(EvolutionError::Validation(
                        "Plan patch postcondition does not retain and require its exact edge evidence"
                            .to_owned(),
                    ));
                }
            }
            _ if !edges.is_empty() => {
                return Err(EvolutionError::Validation(
                    "evolution command introduced an unrelated structural edge".to_owned(),
                ));
            }
            _ => {}
        },
        LiveEvolutionCommand::PublishAndRelink { publication, .. } => {
            let LiveEvolutionOutcome::PublicationApplied { receipt } = outcome else {
                return Err(EvolutionError::Validation(
                    "publication command has no publication outcome".to_owned(),
                ));
            };
            if edges
                .iter()
                .any(|record| record.evidence != publication.evidence.reference)
            {
                return Err(EvolutionError::Validation(
                    "publication edge record does not retain its exact first evidence".to_owned(),
                ));
            }
            let advanced = receipt.updates.iter().any(|update| update.advanced);
            let exact_retention = if advanced {
                artifacts == std::slice::from_ref(&publication.evidence)
            } else {
                artifacts.is_empty()
            };
            if !exact_retention || !required_artifacts.is_empty() {
                return Err(EvolutionError::Validation(
                    "publication evidence Artifact retention does not match its advanced template set"
                        .to_owned(),
                ));
            }
        }
        _ if !edges.is_empty() => {
            return Err(EvolutionError::Validation(
                "evolution command introduced an unrelated structural edge".to_owned(),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn verify_migration_material_authority(
    command: &LiveEvolutionCommand,
    outcome: &LiveEvolutionOutcome,
    mutations: &[EvolutionMutation],
    artifacts: &[ArtifactRecord],
    required_artifacts: &BTreeSet<ArtifactRef>,
) -> EvolutionResult<()> {
    let migration_records = mutations
        .iter()
        .filter(|mutation| matches!(mutation, EvolutionMutation::MigrationRecord(_)))
        .count();
    let LiveEvolutionCommand::Apply { command, .. } = command else {
        if migration_records != 0 {
            return Err(EvolutionError::Validation(
                "non-Apply command introduced a migration record".to_owned(),
            ));
        }
        return Ok(());
    };
    let EvolutionCommand::Migrate { .. } = command.as_ref() else {
        if migration_records != 0 {
            return Err(EvolutionError::Validation(
                "non-migration command introduced a migration record".to_owned(),
            ));
        }
        return Ok(());
    };
    let LiveEvolutionOutcome::Migrated { receipt } = outcome else {
        return Err(EvolutionError::Validation(
            "migration command has no migration outcome".to_owned(),
        ));
    };
    match migration_records {
        0 if artifacts.is_empty() && required_artifacts.is_empty() => Ok(()),
        1 => {
            let source_closed = if receipt.source_binding == receipt.target_binding {
                !required_artifacts.contains(&receipt.source_binding)
            } else {
                required_artifacts.contains(&receipt.source_binding)
            };
            if !source_closed || required_artifacts.contains(&receipt.target_binding) {
                return Err(EvolutionError::Validation(
                    "migration binding Artifact partition is inconsistent".to_owned(),
                ));
            }
            let target = artifacts
                .iter()
                .find(|artifact| artifact.reference == receipt.target_binding)
                .ok_or_else(|| {
                    EvolutionError::Validation(
                        "fresh migration omits its complete target binding Artifact".to_owned(),
                    )
                })?;
            verify_execution_binding_ref(&target.reference)?;
            let binding: cymule_runtime::ExecutionBinding = cymule_core::decode_json(&target.bytes)
                .map_err(|error| {
                    EvolutionError::Validation(format!(
                        "migration target binding Artifact is not strict typed JSON: {error}"
                    ))
                })?;
            binding.verify().map_err(|error| {
                EvolutionError::Validation(format!(
                    "migration target ExecutionBinding is invalid: {error}"
                ))
            })?;
            if binding.canonical_bytes().map_err(|error| {
                EvolutionError::Validation(format!(
                    "migration target ExecutionBinding cannot derive canonical bytes: {error}"
                ))
            })? != target.bytes
            {
                return Err(EvolutionError::Validation(
                    "migration target binding Artifact bytes are not canonical".to_owned(),
                ));
            }
            Ok(())
        }
        _ => Err(EvolutionError::Validation(
            "migration postcondition has an invalid fresh/replay material shape".to_owned(),
        )),
    }
}

fn verify_postcondition_binding(
    current: &EvolutionCurrent,
    alias: &EvolutionCommandAlias,
    receipt: &EvolutionPersistenceReceipt,
) -> EvolutionResult<()> {
    if current.evolution_id != alias.evolution_id
        || current.evolution_id != receipt.command.evolution_id
        || current.last_receipt_id != receipt.receipt_id
        || alias.receipt_id != receipt.receipt_id
        || alias.persistence_id != receipt.command.persistence_id
        || alias.command_id != receipt.command.command.command_id()
    {
        return Err(EvolutionError::Validation(
            "evolution postcondition current, alias, and receipt do not bind".to_owned(),
        ));
    }
    if (current.revision == 1) != receipt.parent_current_id.is_none() {
        return Err(EvolutionError::Validation(
            "evolution current revision does not match receipt parent presence".to_owned(),
        ));
    }
    Ok(())
}

fn verify_postcondition_mutations(
    current: &EvolutionCurrent,
    mutations: &[EvolutionMutation],
) -> EvolutionResult<()> {
    if mutations.len() > MAX_EVOLUTION_TRANSITION_LEAVES {
        return Err(EvolutionError::Validation(
            "evolution postcondition exceeds the normalized mutation bound".to_owned(),
        ));
    }
    let mut previous_key = None;
    for mutation in mutations {
        mutation.verify()?;
        if mutation.evolution_id() != current.evolution_id
            || mutation.revision() != current.revision
        {
            return Err(EvolutionError::Validation(
                "evolution mutation does not belong to the resulting semantic revision".to_owned(),
            ));
        }
        let key = mutation.storage_key()?;
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            return Err(EvolutionError::Validation(
                "evolution mutations are not strictly ordered by exact storage key".to_owned(),
            ));
        }
        previous_key = Some(key);
    }
    Ok(())
}

fn verify_postcondition_plans(plans: &[SealedPlan]) -> EvolutionResult<()> {
    let mut previous = None;
    for plan in plans {
        plan.verify()?;
        if previous.is_some_and(|previous: &String| previous >= &plan.plan_id) {
            return Err(EvolutionError::Validation(
                "evolution introduced Plans are not strictly ordered".to_owned(),
            ));
        }
        previous = Some(&plan.plan_id);
    }
    Ok(())
}

fn verify_postcondition_artifacts(
    artifacts: &[ArtifactRecord],
    required_artifacts: &BTreeSet<ArtifactRef>,
) -> EvolutionResult<()> {
    let mut introduced = BTreeSet::new();
    for artifact in artifacts {
        verify_artifact_record(artifact)?;
        if !introduced.insert(artifact.reference.clone()) {
            return Err(EvolutionError::Validation(
                "evolution postcondition repeats one introduced Artifact".to_owned(),
            ));
        }
    }
    for reference in required_artifacts {
        reference
            .validate()
            .map_err(|error| EvolutionError::Validation(error.to_string()))?;
        if introduced.contains(reference) {
            return Err(EvolutionError::Validation(
                "introduced Artifact is also listed as pre-existing authority".to_owned(),
            ));
        }
    }
    Ok(())
}

fn verify_postcondition_aggregate(
    current: Option<&EvolutionCurrent>,
    alias: Option<&EvolutionCommandAlias>,
    receipt: Option<&EvolutionPersistenceReceipt>,
    mutations: &[EvolutionMutation],
    plans: &[SealedPlan],
    artifacts: &[ArtifactRecord],
    required_artifacts: &BTreeSet<ArtifactRef>,
) -> EvolutionResult<()> {
    let mut total = 0_usize;
    for bytes in current
        .into_iter()
        .map(canonical_bytes)
        .chain(alias.into_iter().map(canonical_bytes))
        .chain(receipt.into_iter().map(canonical_bytes))
        .chain(
            mutations
                .iter()
                .map(canonical_bytes)
                .chain(plans.iter().map(canonical_bytes))
                .chain(artifacts.iter().map(canonical_bytes))
                .chain(required_artifacts.iter().map(canonical_bytes)),
        )
    {
        let bytes = bytes?;
        total = total.checked_add(bytes.len()).ok_or_else(|| {
            EvolutionError::Validation(
                "evolution postcondition canonical byte accounting overflowed".to_owned(),
            )
        })?;
        if total > MAX_EVOLUTION_POSTCONDITION_BYTES {
            return Err(EvolutionError::Validation(format!(
                "evolution postcondition uses {total} aggregate canonical bytes, above the {MAX_EVOLUTION_POSTCONDITION_BYTES} byte bound"
            )));
        }
    }
    Ok(())
}

fn verify_artifact_record(record: &ArtifactRecord) -> EvolutionResult<()> {
    record
        .reference
        .validate()
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    if cymule_core::artifact_ref(&record.reference.kind, &record.bytes)? != record.reference {
        return Err(EvolutionError::Validation(
            "evolution Artifact record bytes do not match their reference".to_owned(),
        ));
    }
    Ok(())
}

fn require_no_provider(provider: &EvolutionProviderAuthorityBody) -> EvolutionResult<()> {
    match provider {
        EvolutionProviderAuthorityBody::None => Ok(()),
        _ => Err(EvolutionError::Conflict(
            "provider authority was supplied to a deterministic evolution command".to_owned(),
        )),
    }
}

fn definition_head_mutations(
    evolution_id: &str,
    revision: u64,
    logical_ref: &str,
    latest: SubflowRevision,
    max_sequence: u64,
    include_record: bool,
) -> EvolutionResult<Vec<EvolutionMutation>> {
    let current = EvolutionDefinitionCurrent {
        leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
        evolution_id: evolution_id.to_owned(),
        revision,
        logical_ref: logical_ref.to_owned(),
        max_sequence,
        latest: latest.clone(),
    };
    let compatible = EvolutionDefinitionCompatibilityCurrent {
        leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
        evolution_id: evolution_id.to_owned(),
        revision,
        logical_ref: logical_ref.to_owned(),
        contract_id: definition_contract_id(
            &latest.definition.input_schema,
            &latest.definition.output_schema,
        )?,
        latest,
    };
    let mut mutations = vec![
        EvolutionMutation::DefinitionCurrent(Box::new(current.clone())),
        EvolutionMutation::DefinitionCompatibilityCurrent(Box::new(compatible)),
    ];
    if include_record {
        mutations.push(EvolutionMutation::DefinitionRecord(Box::new(current)));
    }
    Ok(mutations)
}

struct PreparedDefinitionPublication {
    published: SubflowRevision,
    max_sequence: u64,
    current_changed: bool,
    record_is_new: bool,
}

fn prepare_definition_publication(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    logical_ref: &str,
    definition: &Definition,
    references: &[SubflowReference],
) -> EvolutionResult<PreparedDefinitionPublication> {
    let prior = definition_current(view, evolution_id, logical_ref, "current")?;
    let provisional_sequence = prior.map_or(1, |prior| prior.max_sequence);
    let mut candidate = super::linker::seal_definition_revision(
        logical_ref.to_owned(),
        definition.clone(),
        references.to_vec(),
        provisional_sequence,
    )?;
    let _ = revision_closure(view, evolution_id, references, Some(&candidate))?;
    if let Some(prior) = prior
        && prior.latest.revision_id == candidate.revision_id
    {
        return Ok(PreparedDefinitionPublication {
            published: prior.latest.clone(),
            max_sequence: prior.max_sequence,
            current_changed: false,
            record_is_new: false,
        });
    }
    let historical = definition_record(view, evolution_id, logical_ref, &candidate.revision_id)?;
    if let Some(historical) = historical {
        return Ok(PreparedDefinitionPublication {
            published: historical.latest.clone(),
            max_sequence: prior.map_or(historical.max_sequence, |current| current.max_sequence),
            current_changed: true,
            record_is_new: false,
        });
    }
    let sequence = prior.map_or(Ok(1), |prior| {
        prior
            .max_sequence
            .checked_add(1)
            .filter(|sequence| *sequence <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                EvolutionError::Validation(
                    "subflow revision sequence exhausted the exact range".to_owned(),
                )
            })
    })?;
    candidate.sequence = sequence;
    Ok(PreparedDefinitionPublication {
        published: candidate,
        max_sequence: sequence,
        current_changed: true,
        record_is_new: true,
    })
}

fn reduce_definition_publication(
    view: &EvolutionAuthorityView,
    persistence: &EvolutionPersistenceCommand,
    revision: u64,
    logical_ref: &str,
    definition: &Definition,
    references: &[SubflowReference],
    provider: &EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    require_no_provider(provider)?;
    if let Some(dependencies) =
        dependency_current(view, &persistence.evolution_id, logical_ref, "current")?
        && !dependencies.template_ids.is_empty()
    {
        return Err(EvolutionError::Conflict(format!(
            "definition {logical_ref} has registered dependents and requires publish-and-relink"
        )));
    }
    let prepared = prepare_definition_publication(
        view,
        &persistence.evolution_id,
        logical_ref,
        definition,
        references,
    )?;
    let mutations = if prepared.current_changed {
        definition_head_mutations(
            &persistence.evolution_id,
            revision,
            logical_ref,
            prepared.published.clone(),
            prepared.max_sequence,
            prepared.record_is_new,
        )?
    } else {
        Vec::new()
    };
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::DefinitionPublished {
            revision: prepared.published,
        },
        mutations,
        plans: Vec::new(),
        artifacts: Vec::new(),
        required_artifacts: BTreeSet::new(),
    })
}

fn reduce_template_registration(
    view: &EvolutionAuthorityView,
    persistence: &EvolutionPersistenceCommand,
    revision: u64,
    template: &PlanTemplate,
    provider: &EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    require_no_provider(provider)?;
    if let Some(existing) = template_current(
        view,
        &persistence.evolution_id,
        &template.template_id,
        "current",
    )? {
        if existing.template != *template {
            return Err(EvolutionError::Conflict(format!(
                "template {} already has different content",
                template.template_id
            )));
        }
        return Ok(ReducedEvolution {
            outcome: LiveEvolutionOutcome::TemplateRegistered {
                linked: existing.linked.clone(),
            },
            mutations: Vec::new(),
            plans: Vec::new(),
            artifacts: Vec::new(),
            required_artifacts: BTreeSet::new(),
        });
    }
    let prepared = prepare_template_registration(view, &persistence.evolution_id, template)?;
    let mut mutations = vec![
        EvolutionMutation::TemplateCurrent(Box::new(EvolutionTemplateCurrent {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: persistence.evolution_id.clone(),
            revision,
            template: template.clone(),
            linked: prepared.linked.clone(),
        })),
        EvolutionMutation::LinkRecord(Box::new(EvolutionLinkRecord {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: persistence.evolution_id.clone(),
            revision,
            template_id: template.template_id.clone(),
            link_id: prepared.link_id,
            linked: prepared.linked.clone(),
        })),
        EvolutionMutation::PlanRecord(Box::new(EvolutionPlanRecord {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: persistence.evolution_id.clone(),
            revision,
            template_id: template.template_id.clone(),
            plan: prepared.linked.plan.clone(),
        })),
        EvolutionMutation::RolloutCurrent(Box::new(new_rollout_current(
            &persistence.evolution_id,
            revision,
            &template.template_id,
            prepared.decision.clone(),
        ))),
        EvolutionMutation::RolloutEvidenceCurrent(Box::new(new_rollout_evidence_current(
            &persistence.evolution_id,
            revision,
            &template.template_id,
            &prepared.decision.decision_id,
        )?)),
        EvolutionMutation::RolloutDecision(Box::new(EvolutionRolloutDecisionRecord {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: persistence.evolution_id.clone(),
            revision,
            template_id: template.template_id.clone(),
            decision: prepared.decision,
        })),
    ];
    mutations.extend(template_dependency_mutations(
        view,
        &persistence.evolution_id,
        revision,
        template,
    )?);
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::TemplateRegistered {
            linked: prepared.linked.clone(),
        },
        mutations,
        plans: vec![prepared.linked.plan],
        artifacts: Vec::new(),
        required_artifacts: BTreeSet::new(),
    })
}

struct PreparedTemplateRegistration {
    linked: LinkedPlan,
    link_id: String,
    decision: RolloutDecision,
}

fn prepare_template_registration(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    template: &PlanTemplate,
) -> EvolutionResult<PreparedTemplateRegistration> {
    let revisions = revision_view(view, evolution_id, template, None)?;
    let linked = super::linker::link_from_revision_view(template, &revisions)?;
    let link_id = linked_plan_record_id(&linked)?;
    if plan_record(
        view,
        evolution_id,
        &template.template_id,
        &linked.plan.plan_id,
    )?
    .is_some()
        || link_record(view, evolution_id, &template.template_id, &link_id)?.is_some()
    {
        return Err(EvolutionError::Conflict(
            "new template collides with retained Plan or link authority".to_owned(),
        ));
    }
    if rollout_current(view, evolution_id, &template.template_id, "current")?.is_some() {
        return Err(EvolutionError::Conflict(
            "new template already has a rollout current".to_owned(),
        ));
    }
    let decision = initial_decision(&template.template_id, &linked.plan.plan_id)?;
    if rollout_decision_record(
        view,
        evolution_id,
        &template.template_id,
        &decision.decision_id,
    )?
    .is_some()
    {
        return Err(EvolutionError::Conflict(
            "initial rollout decision already belongs to another transition".to_owned(),
        ));
    }
    Ok(PreparedTemplateRegistration {
        linked,
        link_id,
        decision,
    })
}

fn template_dependency_mutations(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    revision: u64,
    template: &PlanTemplate,
) -> EvolutionResult<Vec<EvolutionMutation>> {
    template
        .references
        .iter()
        .filter_map(|reference| {
            matches!(&reference.strategy, ReferenceStrategy::LatestCompatible)
                .then_some(&reference.logical_ref)
        })
        .map(|logical_ref| {
            let mut template_ids = dependency_current(view, evolution_id, logical_ref, "current")?
                .map_or_else(BTreeSet::new, |current| current.template_ids.clone());
            template_ids.insert(template.template_id.clone());
            if template_ids.len() > MAX_EVOLUTION_PUBLICATION_TEMPLATES {
                return Err(EvolutionError::Validation(format!(
                    "definition {logical_ref} exceeds the bounded dependent-template limit"
                )));
            }
            Ok(EvolutionMutation::DependencyCurrent(Box::new(
                EvolutionDependencyCurrent {
                    leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                    evolution_id: evolution_id.to_owned(),
                    revision,
                    logical_ref: logical_ref.clone(),
                    template_ids,
                },
            )))
        })
        .collect()
}

fn reduce_publish_and_relink(
    view: &EvolutionAuthorityView,
    persistence: &EvolutionPersistenceCommand,
    revision: u64,
    publication: &super::LivePublicationCommand,
    provider: &EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    require_no_provider(provider)?;
    verify_artifact_record(&publication.evidence)?;
    let prepared = prepare_definition_publication(
        view,
        &persistence.evolution_id,
        &publication.logical_ref,
        &publication.definition,
        &publication.references,
    )?;
    let published = &prepared.published;
    let dependencies = dependency_current(
        view,
        &persistence.evolution_id,
        &publication.logical_ref,
        "current",
    )?;
    let affected = dependencies.map_or_else(BTreeSet::new, |value| value.template_ids.clone());
    if affected.len() > MAX_EVOLUTION_PUBLICATION_TEMPLATES {
        return Err(EvolutionError::Validation(
            "definition publication exceeds the bounded dependent-template limit".to_owned(),
        ));
    }
    let mut mutations = Vec::new();
    if prepared.current_changed {
        mutations.extend(definition_head_mutations(
            &persistence.evolution_id,
            revision,
            &publication.logical_ref,
            published.clone(),
            prepared.max_sequence,
            prepared.record_is_new,
        )?);
    }
    let mut updates = Vec::with_capacity(affected.len());
    let mut plans = Vec::new();
    let mut advanced_any = false;
    let context = PublicationRelinkContext {
        view,
        evolution_id: &persistence.evolution_id,
        revision,
        publication,
        published,
    };
    for template_id in affected {
        let reduced = reduce_dependent_template_relink(&context, &template_id)?;
        mutations.extend(reduced.mutations);
        advanced_any |= reduced.update.advanced;
        if let Some(plan) = reduced.introduced_plan {
            plans.push(plan);
        }
        updates.push(reduced.update);
    }
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::PublicationApplied {
            receipt: LivePublicationReceipt {
                revision: prepared.published,
                updates,
            },
        },
        mutations,
        plans,
        artifacts: advanced_any
            .then(|| publication.evidence.clone())
            .into_iter()
            .collect(),
        required_artifacts: BTreeSet::new(),
    })
}

struct PublicationRelinkContext<'a> {
    view: &'a EvolutionAuthorityView,
    evolution_id: &'a str,
    revision: u64,
    publication: &'a super::LivePublicationCommand,
    published: &'a SubflowRevision,
}

struct ReducedTemplateRelink {
    update: LiveTemplateUpdate,
    mutations: Vec<EvolutionMutation>,
    introduced_plan: Option<SealedPlan>,
}

fn reduce_dependent_template_relink(
    context: &PublicationRelinkContext<'_>,
    template_id: &str,
) -> EvolutionResult<ReducedTemplateRelink> {
    let current = template_current(context.view, context.evolution_id, template_id, "current")?
        .ok_or_else(|| {
            EvolutionError::NotFound(format!(
                "dependent template {template_id} has no exact current"
            ))
        })?;
    if !current
        .linked
        .resolved_revisions
        .contains_key(&context.publication.logical_ref)
    {
        return Err(EvolutionError::Conflict(format!(
            "definition {} reverse dependency is inconsistent with template {template_id}",
            context.publication.logical_ref
        )));
    }
    let revisions = revision_view(
        context.view,
        context.evolution_id,
        &current.template,
        Some(context.published),
    )?;
    let linked = if revisions
        .get(&context.publication.logical_ref)
        .is_some_and(|revision| revision.revision_id == context.published.revision_id)
    {
        super::linker::link_from_revision_view(&current.template, &revisions)?
    } else {
        current.linked.clone()
    };
    let compatible = linked.plan.plan_id == current.linked.plan.plan_id
        || analyze_relink(&current.linked.plan, &linked.plan)?.is_compatible();
    let admitted_link = compatible && linked != current.linked;
    let advanced = compatible && linked.plan.plan_id != current.linked.plan.plan_id;
    let mut mutations = if admitted_link {
        relink_current_mutations(context, template_id, current, &linked)?
    } else {
        Vec::new()
    };
    let advanced_relink = advanced
        .then(|| advance_relinked_template(context, template_id, current, &linked))
        .transpose()?;
    let (decision_id, introduced_plan) = advanced_relink.map_or((None, None), |relink| {
        mutations.extend(relink.mutations);
        (Some(relink.decision_id), relink.introduced_plan)
    });
    Ok(ReducedTemplateRelink {
        update: LiveTemplateUpdate {
            template_id: template_id.to_owned(),
            previous_plan_id: current.linked.plan.plan_id.clone(),
            current_plan_id: if advanced {
                linked.plan.plan_id.clone()
            } else {
                current.linked.plan.plan_id.clone()
            },
            decision_id,
            advanced,
        },
        mutations,
        introduced_plan,
    })
}

fn relink_current_mutations(
    context: &PublicationRelinkContext<'_>,
    template_id: &str,
    current: &EvolutionTemplateCurrent,
    linked: &LinkedPlan,
) -> EvolutionResult<Vec<EvolutionMutation>> {
    let link_id = linked_plan_record_id(linked)?;
    let retained = link_record(context.view, context.evolution_id, template_id, &link_id)?;
    if retained.is_some_and(|retained| retained.linked != *linked) {
        return Err(EvolutionError::Conflict(
            "retained link identity has different immutable content".to_owned(),
        ));
    }
    let mut mutations = vec![EvolutionMutation::TemplateCurrent(Box::new(
        EvolutionTemplateCurrent {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: context.evolution_id.to_owned(),
            revision: context.revision,
            template: current.template.clone(),
            linked: linked.clone(),
        },
    ))];
    if retained.is_none() {
        mutations.push(EvolutionMutation::LinkRecord(Box::new(
            EvolutionLinkRecord {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: template_id.to_owned(),
                link_id,
                linked: linked.clone(),
            },
        )));
    }
    Ok(mutations)
}

struct AdvancedTemplateRelink {
    decision_id: String,
    mutations: Vec<EvolutionMutation>,
    introduced_plan: Option<SealedPlan>,
}

fn advance_relinked_template(
    context: &PublicationRelinkContext<'_>,
    template_id: &str,
    current: &EvolutionTemplateCurrent,
    linked: &LinkedPlan,
) -> EvolutionResult<AdvancedTemplateRelink> {
    let retained_plan = plan_record(
        context.view,
        context.evolution_id,
        template_id,
        &linked.plan.plan_id,
    )?;
    if retained_plan.is_some_and(|retained| retained.plan != linked.plan) {
        return Err(EvolutionError::Conflict(
            "retained Plan identity has different immutable content".to_owned(),
        ));
    }
    let rollout = rollout_current(context.view, context.evolution_id, template_id, "current")?
        .ok_or_else(|| EvolutionError::NotFound("rollout current is missing".to_owned()))?;
    let decision = update_decision(
        template_id,
        &rollout.decision.decision_id,
        authoritative_fallback(&rollout.decision),
        &linked.plan.plan_id,
        context.publication.mode.clone(),
    )?;
    if rollout_decision_record(
        context.view,
        context.evolution_id,
        template_id,
        &decision.decision_id,
    )?
    .is_some()
    {
        return Err(EvolutionError::Conflict(
            "relink rollout decision already exists".to_owned(),
        ));
    }
    let edge = build_relink_edge(context, template_id, current, linked)?;
    let decision_id = decision.decision_id.clone();
    let introduced_plan = retained_plan.is_none().then(|| linked.plan.clone());
    Ok(AdvancedTemplateRelink {
        decision_id,
        mutations: relink_advance_mutations(
            context,
            template_id,
            linked,
            retained_plan.is_none(),
            edge,
            decision,
        )?,
        introduced_plan,
    })
}

struct PreparedRelinkEdge {
    edge: PlanEdge,
    first_evidence: ArtifactRef,
    is_new: bool,
}

fn build_relink_edge(
    context: &PublicationRelinkContext<'_>,
    template_id: &str,
    current: &EvolutionTemplateCurrent,
    linked: &LinkedPlan,
) -> EvolutionResult<PreparedRelinkEdge> {
    let operations = super::diff_plans(&current.linked.plan, &linked.plan)?;
    if operations.is_empty() {
        return Err(EvolutionError::Validation(
            "relinked Plan has no semantic change".to_owned(),
        ));
    }
    let edge = PlanEdge {
        edge_id: super::controller::derive_plan_edge_id(
            &current.linked.plan.plan_id,
            &linked.plan.plan_id,
            &operations,
        )?,
        from_plan: current.linked.plan.plan_id.clone(),
        to_plan: linked.plan.plan_id.clone(),
        operations,
    };
    let retained = edge_record(
        context.view,
        context.evolution_id,
        template_id,
        &edge.edge_id,
    )?;
    if let Some(retained) = retained {
        if retained.edge != edge {
            return Err(EvolutionError::Conflict(
                "retained relink edge identity has different structural content".to_owned(),
            ));
        }
        return Ok(PreparedRelinkEdge {
            edge: retained.edge.clone(),
            first_evidence: retained.evidence.clone(),
            is_new: false,
        });
    }
    Ok(PreparedRelinkEdge {
        edge,
        first_evidence: context.publication.evidence.reference.clone(),
        is_new: true,
    })
}

fn relink_advance_mutations(
    context: &PublicationRelinkContext<'_>,
    template_id: &str,
    linked: &LinkedPlan,
    insert_plan: bool,
    edge: PreparedRelinkEdge,
    decision: RolloutDecision,
) -> EvolutionResult<Vec<EvolutionMutation>> {
    let mut mutations = Vec::new();
    if insert_plan {
        mutations.push(EvolutionMutation::PlanRecord(Box::new(
            EvolutionPlanRecord {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: template_id.to_owned(),
                plan: linked.plan.clone(),
            },
        )));
    }
    if edge.is_new {
        mutations.push(EvolutionMutation::EdgeRecord(Box::new(
            EvolutionEdgeRecord {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: template_id.to_owned(),
                edge: edge.edge,
                evidence: edge.first_evidence,
            },
        )));
    }
    mutations.extend([
        EvolutionMutation::RolloutCurrent(Box::new(new_rollout_current(
            context.evolution_id,
            context.revision,
            template_id,
            decision.clone(),
        ))),
        EvolutionMutation::RolloutEvidenceCurrent(Box::new(new_rollout_evidence_current(
            context.evolution_id,
            context.revision,
            template_id,
            &decision.decision_id,
        )?)),
        EvolutionMutation::RolloutDecision(Box::new(EvolutionRolloutDecisionRecord {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: context.evolution_id.to_owned(),
            revision: context.revision,
            template_id: template_id.to_owned(),
            decision,
        })),
    ]);
    Ok(mutations)
}

fn reduce_patch_command(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    revision: u64,
    template_id: &str,
    patch: &super::PlanPatch,
    provider: &EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    require_no_provider(provider)?;
    let parent = plan_record(view, evolution_id, template_id, &patch.from_plan)?
        .ok_or_else(|| EvolutionError::NotFound("patch parent Plan is missing".to_owned()))?;
    let child = cymule_core::seal_plan(patch.target.clone())?;
    if child.plan_id == parent.plan.plan_id {
        return Err(EvolutionError::Validation(
            "Plan patch contains no semantic change".to_owned(),
        ));
    }
    let operations = super::diff_plans(&parent.plan, &child)?;
    if operations.is_empty() || operations != patch.operations {
        return Err(EvolutionError::Conflict(
            "declared Plan patch does not match the deterministic structural diff".to_owned(),
        ));
    }
    if plan_record(view, evolution_id, template_id, &child.plan_id)?.is_some() {
        return Err(EvolutionError::Conflict(
            "patch target Plan already has authority; every new edge requires a new target"
                .to_owned(),
        ));
    }
    let edge = PlanEdge {
        edge_id: super::controller::derive_plan_edge_id(
            &patch.from_plan,
            &child.plan_id,
            &operations,
        )?,
        from_plan: patch.from_plan.clone(),
        to_plan: child.plan_id.clone(),
        operations,
    };
    if edge_record(view, evolution_id, template_id, &edge.edge_id)?.is_some() {
        return Err(EvolutionError::Conflict(
            "patch edge already has authority".to_owned(),
        ));
    }
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::PatchApplied { edge: edge.clone() },
        mutations: vec![
            EvolutionMutation::PlanRecord(Box::new(EvolutionPlanRecord {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: evolution_id.to_owned(),
                revision,
                template_id: template_id.to_owned(),
                plan: child.clone(),
            })),
            EvolutionMutation::EdgeRecord(Box::new(EvolutionEdgeRecord {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: evolution_id.to_owned(),
                revision,
                template_id: template_id.to_owned(),
                edge,
                evidence: patch.evidence.clone(),
            })),
        ],
        plans: vec![child],
        artifacts: Vec::new(),
        required_artifacts: BTreeSet::from([patch.evidence.clone()]),
    })
}

fn reduce_rollout_command(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    revision: u64,
    template_id: &str,
    decision: &RolloutDecision,
    provider: &EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    require_no_provider(provider)?;
    validate_rollout_decision(view, evolution_id, template_id, decision)?;
    let existing = rollout_decision_record(view, evolution_id, template_id, &decision.decision_id)?;
    if existing.is_some_and(|existing| existing.decision != *decision) {
        return Err(EvolutionError::Conflict(
            "rollout decision identity was reused with different content".to_owned(),
        ));
    }
    if decision_transition_current(view, evolution_id, template_id, &decision.decision_id)?
        .is_some()
    {
        return Err(EvolutionError::Conflict(
            "a completed rollout decision cannot become current again".to_owned(),
        ));
    }
    let current = rollout_current(view, evolution_id, template_id, "current")?
        .ok_or_else(|| EvolutionError::NotFound("rollout current is missing".to_owned()))?;
    let mut mutations = Vec::new();
    if current.decision != *decision {
        mutations.push(EvolutionMutation::RolloutCurrent(Box::new(
            new_rollout_current(evolution_id, revision, template_id, decision.clone()),
        )));
    }
    if existing.is_none() {
        mutations.extend([
            EvolutionMutation::RolloutDecision(Box::new(EvolutionRolloutDecisionRecord {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: evolution_id.to_owned(),
                revision,
                template_id: template_id.to_owned(),
                decision: decision.clone(),
            })),
            EvolutionMutation::RolloutEvidenceCurrent(Box::new(new_rollout_evidence_current(
                evolution_id,
                revision,
                template_id,
                &decision.decision_id,
            )?)),
        ]);
    }
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::Applied,
        mutations,
        plans: Vec::new(),
        artifacts: Vec::new(),
        required_artifacts: BTreeSet::new(),
    })
}

struct TemplateCommandContext<'a> {
    view: &'a EvolutionAuthorityView,
    evolution_id: &'a str,
    revision: u64,
    template_id: &'a str,
    source: &'a EvolutionReductionSourceBody,
}

fn reduce_occurrence_selection(
    context: &TemplateCommandContext<'_>,
    occurrence_id: &str,
    selection_id: &str,
    requested_binding: &ArtifactRef,
    provider: &EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    require_no_provider(provider)?;
    let EvolutionReductionSourceBody::Selection {
        plan_id,
        execution_binding,
    } = context.source
    else {
        return Err(EvolutionError::Validation(
            "occurrence reducer has no Durable-derived binding authority".to_owned(),
        ));
    };
    let lineage = occurrence_selection_lineage(
        context.view,
        context.evolution_id,
        context.template_id,
        occurrence_id,
        selection_id,
    )?;
    if lineage.plan.plan_id != *plan_id {
        return Err(EvolutionError::Conflict(
            "occurrence binding authority targets a different selected Plan".to_owned(),
        ));
    }
    if execution_binding.reference != *requested_binding {
        return Err(EvolutionError::Conflict(
            "loaded ExecutionBinding record does not match the semantic selection".to_owned(),
        ));
    }
    let mut mutations = Vec::new();
    let pin = if let Some(existing) = lineage.existing_pin {
        if existing.execution_binding != execution_binding.reference {
            return Err(EvolutionError::Conflict(
                "retained occurrence has a different execution binding".to_owned(),
            ));
        }
        existing
    } else {
        let pin = OccurrencePin {
            occurrence_id: occurrence_id.to_owned(),
            template_id: context.template_id.to_owned(),
            decision_id: lineage.decision_id,
            plan_id: lineage.plan.plan_id,
            execution_binding: execution_binding.reference.clone(),
            selection_id: selection_id.to_owned(),
        };
        mutations.extend([
            EvolutionMutation::OccurrenceCurrent(Box::new(EvolutionOccurrenceCurrent {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                pin: pin.clone(),
            })),
            EvolutionMutation::SelectionCurrent(Box::new(EvolutionSelectionCurrent {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: context.template_id.to_owned(),
                selection_id: selection_id.to_owned(),
                occurrence_id: occurrence_id.to_owned(),
                execution_binding: execution_binding.reference.clone(),
                decision_id: pin.decision_id.clone(),
                plan_id: pin.plan_id.clone(),
            })),
        ]);
        pin
    };
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::OccurrenceSelected { pin },
        mutations,
        plans: Vec::new(),
        artifacts: Vec::new(),
        required_artifacts: BTreeSet::from([execution_binding.reference.clone()]),
    })
}

fn reduce_migration_command(
    context: &TemplateCommandContext<'_>,
    request: &super::MigrationRequest,
    provider: EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    if let Some(existing) = migration_record(
        context.view,
        context.evolution_id,
        context.template_id,
        &request.migration_id,
    )? {
        require_no_provider(&provider)?;
        let EvolutionReductionSourceBody::RetainedMigration { target_binding } = context.source
        else {
            return Err(EvolutionError::Conflict(
                "retained migration replay received fresh or unrelated runtime authority"
                    .to_owned(),
            ));
        };
        if existing.receipt.request != *request
            || existing.receipt.target_binding != target_binding.reference
        {
            return Err(EvolutionError::Conflict(
                "migration identity was reused with different semantics".to_owned(),
            ));
        }
        return Ok(ReducedEvolution {
            outcome: LiveEvolutionOutcome::Migrated {
                receipt: Box::new(existing.receipt.clone()),
            },
            mutations: Vec::new(),
            plans: Vec::new(),
            artifacts: Vec::new(),
            required_artifacts: BTreeSet::new(),
        });
    }
    let EvolutionReductionSourceBody::Migration {
        safe_point,
        continuation,
        source_binding,
        target_binding,
    } = context.source
    else {
        return Err(EvolutionError::Validation(
            "fresh migration reducer has no Durable-derived runtime source".to_owned(),
        ));
    };
    validate_migration_preflight(
        context.view,
        context.evolution_id,
        context.template_id,
        request,
        context.source,
    )?;
    let EvolutionProviderAuthorityBody::Migration(authority) = provider else {
        return Err(EvolutionError::Validation(
            "migration reducer did not receive its provider authority".to_owned(),
        ));
    };
    reduce_new_migration(
        context,
        request,
        safe_point,
        continuation,
        source_binding,
        target_binding,
        *authority,
    )
}

fn reduce_new_migration(
    context: &TemplateCommandContext<'_>,
    request: &super::MigrationRequest,
    safe_point: &MigrationSafePoint,
    continuation: &cymule_durable_protocol::Continuation,
    source_binding: &ArtifactRef,
    target_binding: &ArtifactRecord,
    authority: MigrationProviderAuthority,
) -> EvolutionResult<ReducedEvolution> {
    let MigrationProviderAuthority {
        receipt,
        mut artifacts,
    } = authority;
    if receipt.request != *request {
        return Err(EvolutionError::Conflict(
            "migration provider authority does not match the command".to_owned(),
        ));
    }
    if receipt.source_witness_id != safe_point.safe_point_id
        || receipt.source_binding != *source_binding
        || receipt.target_binding != target_binding.reference
        || receipt.source_execution_fence != continuation.execution_fence
    {
        return Err(EvolutionError::Conflict(
            "migration provider authority does not bind the exact Durable source".to_owned(),
        ));
    }
    let adapter_request = migration_adapter_request(
        request,
        safe_point,
        continuation,
        source_binding,
        target_binding,
    )?;
    let closure = super::adapters::migration_artifact_closure(
        &adapter_request,
        &receipt.target_continuation,
        &receipt.evidence,
    )?;
    let provider_refs = artifacts
        .iter()
        .map(|record| {
            verify_artifact_record(record)?;
            Ok(record.reference.clone())
        })
        .collect::<EvolutionResult<BTreeSet<_>>>()?;
    if provider_refs != closure.plugin_records {
        return Err(EvolutionError::Validation(
            "migration provider Artifact records do not equal their exact closure".to_owned(),
        ));
    }
    artifacts.push(target_binding.clone());
    let mut required_artifacts = closure.retained;
    required_artifacts.extend(continuation_artifacts(continuation)?);
    required_artifacts.insert(source_binding.clone());
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::Migrated {
            receipt: Box::new(receipt.clone()),
        },
        mutations: vec![EvolutionMutation::MigrationRecord(Box::new(
            EvolutionMigrationRecord {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: context.template_id.to_owned(),
                receipt,
            },
        ))],
        plans: Vec::new(),
        artifacts,
        required_artifacts,
    })
}

fn reduce_restart_command(
    context: &TemplateCommandContext<'_>,
    request: &super::RestartRequest,
    provider: &EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    require_no_provider(provider)?;
    let EvolutionReductionSourceBody::Restart {
        safe_point,
        continuation,
    } = context.source
    else {
        return Err(EvolutionError::Validation(
            "restart reducer has no Durable-derived runtime source".to_owned(),
        ));
    };
    validate_restart_preflight(request, safe_point, continuation)?;
    if let Some(existing) = restart_record(
        context.view,
        context.evolution_id,
        context.template_id,
        &request.restart_id,
    )? {
        if existing.receipt.request != *request
            || existing.receipt.source_witness_id != safe_point.safe_point_id
        {
            return Err(EvolutionError::Conflict(
                "restart identity was reused with different semantics".to_owned(),
            ));
        }
        return Ok(ReducedEvolution {
            outcome: LiveEvolutionOutcome::RestartAuthorized {
                receipt: Box::new(existing.receipt.clone()),
            },
            mutations: Vec::new(),
            plans: Vec::new(),
            artifacts: Vec::new(),
            required_artifacts: BTreeSet::new(),
        });
    }
    let source_plan = plan_record(
        context.view,
        context.evolution_id,
        context.template_id,
        &request.from_plan,
    )?
    .ok_or_else(|| EvolutionError::NotFound("restart source Plan is missing".to_owned()))?;
    super::controller::verify_target_program_counters(&source_plan.plan, continuation)?;
    let target = plan_record(
        context.view,
        context.evolution_id,
        context.template_id,
        &request.to_plan,
    )?
    .ok_or_else(|| EvolutionError::NotFound("restart target Plan is missing".to_owned()))?;
    let receipt = RestartReceipt {
        request: request.clone(),
        source_witness_id: safe_point.safe_point_id.clone(),
        target_plan: target.plan.clone(),
    };
    let mut required_artifacts = continuation_artifacts(continuation)?;
    required_artifacts.insert(request.input.clone());
    required_artifacts.insert(request.evidence.clone());
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::RestartAuthorized {
            receipt: Box::new(receipt.clone()),
        },
        mutations: vec![EvolutionMutation::RestartRecord(Box::new(
            EvolutionRestartRecord {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: context.template_id.to_owned(),
                receipt,
            },
        ))],
        plans: Vec::new(),
        artifacts: Vec::new(),
        required_artifacts,
    })
}

fn reduce_shadow_command(
    context: &TemplateCommandContext<'_>,
    request: &super::ShadowRequest,
    provider: EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    let decision = rollout_decision_record(
        context.view,
        context.evolution_id,
        context.template_id,
        &request.decision_id,
    )?
    .ok_or_else(|| EvolutionError::NotFound("shadow rollout decision is missing".to_owned()))?;
    if decision.decision.fallback_plan != request.primary_plan
        || decision.decision.target_plan != request.shadow_plan
    {
        return Err(EvolutionError::Conflict(
            "shadow request does not match its rollout Plan pair".to_owned(),
        ));
    }
    let rollout = rollout_evidence_current(
        context.view,
        context.evolution_id,
        context.template_id,
        &request.decision_id,
    )?
    .ok_or_else(|| EvolutionError::NotFound("shadow rollout evidence is missing".to_owned()))?;
    let subject_identity = shadow_subject_identity(&request.decision_id, &request.subject)?;
    let subject = shadow_subject_current(
        context.view,
        context.evolution_id,
        context.template_id,
        &subject_identity,
    )?;
    let evidence_alias = evidence_current(
        context.view,
        context.evolution_id,
        context.template_id,
        &request.comparison_id,
    )?;
    if let Some(existing) = shadow_record(
        context.view,
        context.evolution_id,
        context.template_id,
        &request.comparison_id,
    )? {
        require_no_provider(&provider)?;
        if !shadow_matches_request(&existing.comparison, request)
            || subject.is_none_or(|subject| {
                subject.comparison_id != request.comparison_id
                    || subject.decision_id != request.decision_id
                    || subject.subject != request.subject
            })
            || evidence_alias.is_none_or(|alias| alias.kind != EvolutionEvidenceKind::Shadow)
        {
            return Err(EvolutionError::Conflict(
                "retained shadow record or its exact aliases do not match the request".to_owned(),
            ));
        }
        return Ok(ReducedEvolution {
            outcome: LiveEvolutionOutcome::ShadowRecorded {
                comparison: existing.comparison.clone(),
            },
            mutations: Vec::new(),
            plans: Vec::new(),
            artifacts: Vec::new(),
            required_artifacts: BTreeSet::new(),
        });
    }
    if subject.is_some() || evidence_alias.is_some() {
        return Err(EvolutionError::Conflict(
            "shadow semantic aliases already belong to another record".to_owned(),
        ));
    }
    let EvolutionProviderAuthorityBody::Shadow(authority) = provider else {
        return Err(EvolutionError::Validation(
            "shadow reducer did not receive its provider authority".to_owned(),
        ));
    };
    reduce_new_shadow(context, request, rollout, *authority)
}

fn reduce_new_shadow(
    context: &TemplateCommandContext<'_>,
    request: &super::ShadowRequest,
    rollout: &EvolutionRolloutEvidenceCurrent,
    authority: ShadowProviderAuthority,
) -> EvolutionResult<ReducedEvolution> {
    let ShadowProviderAuthority {
        comparison,
        evidence,
    } = authority;
    if !shadow_matches_request(&comparison, request) || comparison.evidence != evidence.reference {
        return Err(EvolutionError::Conflict(
            "shadow provider authority does not match the command".to_owned(),
        ));
    }
    verify_artifact_record(&evidence)?;
    let next_rollout = advance_shadow_rollout(rollout, context.revision, &comparison)?;
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::ShadowRecorded {
            comparison: comparison.clone(),
        },
        mutations: vec![
            EvolutionMutation::ShadowRecord(Box::new(EvolutionShadowRecord {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: context.template_id.to_owned(),
                comparison,
            })),
            EvolutionMutation::ShadowSubjectCurrent(Box::new(EvolutionShadowSubjectCurrent {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: context.template_id.to_owned(),
                decision_id: request.decision_id.clone(),
                subject: request.subject.clone(),
                comparison_id: request.comparison_id.clone(),
            })),
            EvolutionMutation::EvidenceCurrent(Box::new(EvolutionEvidenceCurrent {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: context.template_id.to_owned(),
                evidence_id: request.comparison_id.clone(),
                kind: EvolutionEvidenceKind::Shadow,
            })),
            EvolutionMutation::RolloutEvidenceCurrent(Box::new(next_rollout)),
        ],
        plans: Vec::new(),
        artifacts: vec![evidence],
        required_artifacts: BTreeSet::from([request.input.clone()]),
    })
}

fn reduce_observation_command(
    context: &TemplateCommandContext<'_>,
    observation: &RolloutObservation,
    provider: &EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    require_no_provider(provider)?;
    let evidence_alias = evidence_current(
        context.view,
        context.evolution_id,
        context.template_id,
        &observation.observation_id,
    )?;
    let occurrence_alias = observation_occurrence_current(
        context.view,
        context.evolution_id,
        context.template_id,
        &observation.occurrence_id,
    )?;
    if let Some(existing) = observation_record(
        context.view,
        context.evolution_id,
        context.template_id,
        &observation.observation_id,
    )? {
        if existing.observation != *observation
            || evidence_alias.is_none_or(|alias| alias.kind != EvolutionEvidenceKind::Observation)
            || occurrence_alias.is_none_or(|alias| {
                alias.decision_id != observation.decision_id
                    || alias.observation_id != observation.observation_id
            })
        {
            return Err(EvolutionError::Conflict(
                "retained observation or its exact aliases do not match the request".to_owned(),
            ));
        }
        return Ok(ReducedEvolution {
            outcome: LiveEvolutionOutcome::Applied,
            mutations: Vec::new(),
            plans: Vec::new(),
            artifacts: Vec::new(),
            required_artifacts: BTreeSet::new(),
        });
    }
    if evidence_alias.is_some() || occurrence_alias.is_some() {
        return Err(EvolutionError::Conflict(
            "observation semantic aliases already belong to another record".to_owned(),
        ));
    }
    reduce_new_observation(context, observation)
}

fn reduce_new_observation(
    context: &TemplateCommandContext<'_>,
    observation: &RolloutObservation,
) -> EvolutionResult<ReducedEvolution> {
    let pin = occurrence_current(
        context.view,
        context.evolution_id,
        context.template_id,
        &observation.occurrence_id,
    )?
    .ok_or_else(|| EvolutionError::NotFound("observation occurrence pin is missing".to_owned()))?;
    if pin.pin.decision_id != observation.decision_id || pin.pin.plan_id != observation.plan_id {
        return Err(EvolutionError::Conflict(
            "observation does not match its occurrence pin".to_owned(),
        ));
    }
    let decision = rollout_decision_record(
        context.view,
        context.evolution_id,
        context.template_id,
        &observation.decision_id,
    )?
    .ok_or_else(|| EvolutionError::NotFound("observation decision is missing".to_owned()))?;
    if observation.plan_id != decision.decision.fallback_plan
        && observation.plan_id != decision.decision.target_plan
    {
        return Err(EvolutionError::Conflict(
            "observation Plan is outside its rollout decision".to_owned(),
        ));
    }
    let mut mutations = observation_mutations(context, observation);
    if observation.plan_id == decision.decision.target_plan {
        let rollout = rollout_evidence_current(
            context.view,
            context.evolution_id,
            context.template_id,
            &observation.decision_id,
        )?
        .ok_or_else(|| {
            EvolutionError::NotFound("observation rollout evidence is missing".to_owned())
        })?;
        mutations.push(EvolutionMutation::RolloutEvidenceCurrent(Box::new(
            advance_observation_rollout(rollout, context.revision, observation)?,
        )));
    }
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::Applied,
        mutations,
        plans: Vec::new(),
        artifacts: Vec::new(),
        required_artifacts: BTreeSet::from([observation.evidence.clone()]),
    })
}

fn observation_mutations(
    context: &TemplateCommandContext<'_>,
    observation: &RolloutObservation,
) -> Vec<EvolutionMutation> {
    vec![
        EvolutionMutation::ObservationRecord(Box::new(EvolutionObservationRecord {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: context.evolution_id.to_owned(),
            revision: context.revision,
            template_id: context.template_id.to_owned(),
            observation: observation.clone(),
        })),
        EvolutionMutation::ObservationOccurrenceCurrent(Box::new(
            EvolutionObservationOccurrenceCurrent {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: context.template_id.to_owned(),
                decision_id: observation.decision_id.clone(),
                occurrence_id: observation.occurrence_id.clone(),
                observation_id: observation.observation_id.clone(),
            },
        )),
        EvolutionMutation::EvidenceCurrent(Box::new(EvolutionEvidenceCurrent {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: context.evolution_id.to_owned(),
            revision: context.revision,
            template_id: context.template_id.to_owned(),
            evidence_id: observation.observation_id.clone(),
            kind: EvolutionEvidenceKind::Observation,
        })),
    ]
}

fn reduce_gate_command(
    context: &TemplateCommandContext<'_>,
    gate: &RolloutGate,
    next_decision_id: &str,
    provider: &EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    require_no_provider(provider)?;
    let rollout = rollout_current(
        context.view,
        context.evolution_id,
        context.template_id,
        "current",
    )?
    .ok_or_else(|| EvolutionError::NotFound("gate rollout is missing".to_owned()))?;
    let evidence = rollout_evidence_current(
        context.view,
        context.evolution_id,
        context.template_id,
        &rollout.decision.decision_id,
    )?
    .ok_or_else(|| EvolutionError::NotFound("gate rollout evidence is missing".to_owned()))?;
    if decision_transition_current(
        context.view,
        context.evolution_id,
        context.template_id,
        &rollout.decision.decision_id,
    )?
    .is_some()
    {
        return Err(EvolutionError::Conflict(
            "current rollout decision already has a completed transition".to_owned(),
        ));
    }
    let (transition, decision) =
        evaluate_rollout_gate(&rollout.decision, evidence, gate, next_decision_id)?;
    let existing = transition_record(
        context.view,
        context.evolution_id,
        context.template_id,
        &transition.transition_id,
    )?;
    if existing.is_some_and(|existing| existing.transition != transition) {
        return Err(EvolutionError::Conflict(
            "rollout transition identity was reused".to_owned(),
        ));
    }
    let mutations = if existing.is_some() {
        Vec::new()
    } else {
        if rollout_decision_record(
            context.view,
            context.evolution_id,
            context.template_id,
            &decision.decision_id,
        )?
        .is_some()
        {
            return Err(EvolutionError::Conflict(
                "gate target decision already has authority".to_owned(),
            ));
        }
        gate_mutations(context, next_decision_id, transition.clone(), decision)?
    };
    Ok(ReducedEvolution {
        outcome: LiveEvolutionOutcome::GateApplied { transition },
        mutations,
        plans: Vec::new(),
        artifacts: Vec::new(),
        required_artifacts: BTreeSet::new(),
    })
}

fn gate_mutations(
    context: &TemplateCommandContext<'_>,
    next_decision_id: &str,
    transition: RolloutTransition,
    decision: RolloutDecision,
) -> EvolutionResult<Vec<EvolutionMutation>> {
    Ok(vec![
        EvolutionMutation::TransitionRecord(Box::new(EvolutionTransitionRecord {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: context.evolution_id.to_owned(),
            revision: context.revision,
            template_id: context.template_id.to_owned(),
            transition: transition.clone(),
        })),
        EvolutionMutation::DecisionTransitionCurrent(Box::new(
            EvolutionDecisionTransitionCurrent {
                leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
                evolution_id: context.evolution_id.to_owned(),
                revision: context.revision,
                template_id: context.template_id.to_owned(),
                source_decision_id: transition.from_decision.clone(),
                transition_id: transition.transition_id,
            },
        )),
        EvolutionMutation::RolloutDecision(Box::new(EvolutionRolloutDecisionRecord {
            leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
            evolution_id: context.evolution_id.to_owned(),
            revision: context.revision,
            template_id: context.template_id.to_owned(),
            decision: decision.clone(),
        })),
        EvolutionMutation::RolloutCurrent(Box::new(new_rollout_current(
            context.evolution_id,
            context.revision,
            context.template_id,
            decision,
        ))),
        EvolutionMutation::RolloutEvidenceCurrent(Box::new(new_rollout_evidence_current(
            context.evolution_id,
            context.revision,
            context.template_id,
            next_decision_id,
        )?)),
    ])
}

fn reduce_template_command(
    view: &EvolutionAuthorityView,
    persistence: &EvolutionPersistenceCommand,
    revision: u64,
    template_id: &str,
    command: &EvolutionCommand,
    source: &EvolutionReductionSourceBody,
    provider: EvolutionProviderAuthorityBody,
) -> EvolutionResult<ReducedEvolution> {
    if template_current(view, &persistence.evolution_id, template_id, "current")?.is_none() {
        return Err(EvolutionError::NotFound(format!(
            "live evolution template {template_id} is missing"
        )));
    }
    let context = TemplateCommandContext {
        view,
        evolution_id: &persistence.evolution_id,
        revision,
        template_id,
        source,
    };
    match command {
        EvolutionCommand::ApplyPatch { patch, .. } => reduce_patch_command(
            view,
            &persistence.evolution_id,
            revision,
            template_id,
            patch,
            &provider,
        ),
        EvolutionCommand::SetRollout { decision, .. } => reduce_rollout_command(
            view,
            &persistence.evolution_id,
            revision,
            template_id,
            decision,
            &provider,
        ),
        EvolutionCommand::SelectOccurrence {
            occurrence_id,
            selection_id,
            execution_binding: requested_binding,
            ..
        } => reduce_occurrence_selection(
            &context,
            occurrence_id,
            selection_id,
            requested_binding,
            &provider,
        ),
        EvolutionCommand::Migrate { request, .. } => {
            reduce_migration_command(&context, request, provider)
        }
        EvolutionCommand::RestartUnderNewPlan { request, .. } => {
            reduce_restart_command(&context, request, &provider)
        }
        EvolutionCommand::Shadow { request, .. } => {
            reduce_shadow_command(&context, request, provider)
        }
        EvolutionCommand::Observe { observation, .. } => {
            reduce_observation_command(&context, observation, &provider)
        }
        EvolutionCommand::ApplyGate {
            gate,
            next_decision_id,
            ..
        } => reduce_gate_command(&context, gate, next_decision_id, &provider),
    }
}

fn revision_closure(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    references: &[SubflowReference],
    override_revision: Option<&SubflowRevision>,
) -> EvolutionResult<BTreeMap<String, SubflowRevision>> {
    let mut revisions = BTreeMap::new();
    for reference in references {
        resolve_revision_reference(
            view,
            evolution_id,
            reference,
            override_revision,
            &mut revisions,
            &mut BTreeSet::new(),
            0,
        )?;
    }
    Ok(revisions)
}

fn resolve_revision_reference(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    reference: &SubflowReference,
    override_revision: Option<&SubflowRevision>,
    resolved: &mut BTreeMap<String, SubflowRevision>,
    visiting: &mut BTreeSet<String>,
    depth: usize,
) -> EvolutionResult<()> {
    if depth >= super::MAX_SUBFLOW_REFERENCE_DEPTH {
        return Err(EvolutionError::Validation(format!(
            "reusable definition dependency depth exceeds {}",
            super::MAX_SUBFLOW_REFERENCE_DEPTH
        )));
    }
    if !visiting.insert(reference.logical_ref.clone()) {
        return Err(EvolutionError::Conflict(format!(
            "reusable definition dependency cycle reaches {}",
            reference.logical_ref
        )));
    }
    let selected = select_reference_revision(view, evolution_id, reference, override_revision)?;
    if selected.definition.input_schema != reference.input_schema
        || selected.definition.output_schema != reference.output_schema
    {
        return Err(EvolutionError::Conflict(format!(
            "subflow reference {} has no compatible selected revision",
            reference.logical_ref
        )));
    }
    match resolved.get(&reference.logical_ref) {
        Some(existing) if existing.revision_id != selected.revision_id => {
            return Err(EvolutionError::Conflict(format!(
                "subflow reference {} resolves to conflicting revisions",
                reference.logical_ref
            )));
        }
        Some(_) => {
            visiting.remove(&reference.logical_ref);
            return Ok(());
        }
        None => {
            resolved.insert(reference.logical_ref.clone(), selected.clone());
        }
    }
    for dependency in &selected.references {
        resolve_revision_reference(
            view,
            evolution_id,
            dependency,
            override_revision,
            resolved,
            visiting,
            depth + 1,
        )?;
    }
    visiting.remove(&reference.logical_ref);
    Ok(())
}

fn select_reference_revision(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    reference: &SubflowReference,
    override_revision: Option<&SubflowRevision>,
) -> EvolutionResult<SubflowRevision> {
    if let Some(revision) = override_revision
        && revision.logical_ref == reference.logical_ref
        && revision.definition.input_schema == reference.input_schema
        && revision.definition.output_schema == reference.output_schema
        && match &reference.strategy {
            ReferenceStrategy::LatestCompatible => true,
            ReferenceStrategy::Pinned { revision_id } => revision_id == &revision.revision_id,
        }
    {
        return Ok(revision.clone());
    }
    match &reference.strategy {
        ReferenceStrategy::LatestCompatible => {
            let contract_id =
                definition_contract_id(&reference.input_schema, &reference.output_schema)?;
            definition_compatibility_current(
                view,
                evolution_id,
                &reference.logical_ref,
                &contract_id,
            )?
            .map(|value| value.latest.clone())
            .ok_or_else(|| {
                EvolutionError::NotFound(format!(
                    "subflow reference {} has no compatible current revision",
                    reference.logical_ref
                ))
            })
        }
        ReferenceStrategy::Pinned { revision_id } => {
            definition_record(view, evolution_id, &reference.logical_ref, revision_id)?
                .map(|value| value.latest.clone())
                .ok_or_else(|| {
                    EvolutionError::NotFound(format!(
                        "pinned subflow revision {revision_id} is missing"
                    ))
                })
        }
    }
}

fn revision_view(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    template: &PlanTemplate,
    override_revision: Option<&SubflowRevision>,
) -> EvolutionResult<BTreeMap<String, SubflowRevision>> {
    revision_closure(view, evolution_id, &template.references, override_revision)
}

fn initial_decision(template_id: &str, plan_id: &str) -> EvolutionResult<RolloutDecision> {
    Ok(RolloutDecision {
        decision_id: content_id(INITIAL_DECISION_ID_DOMAIN, &(template_id, plan_id))?,
        fallback_plan: plan_id.to_owned(),
        target_plan: plan_id.to_owned(),
        mode: RolloutMode::Active,
    })
}

fn update_decision(
    template_id: &str,
    source_decision_id: &str,
    fallback_plan: &str,
    target_plan: &str,
    mode: RolloutMode,
) -> EvolutionResult<RolloutDecision> {
    Ok(RolloutDecision {
        decision_id: content_id(
            UPDATE_DECISION_ID_DOMAIN,
            &(
                template_id,
                source_decision_id,
                fallback_plan,
                target_plan,
                &mode,
            ),
        )?,
        fallback_plan: fallback_plan.to_owned(),
        target_plan: target_plan.to_owned(),
        mode,
    })
}

fn authoritative_fallback(decision: &RolloutDecision) -> &str {
    match decision.mode {
        RolloutMode::Active => &decision.target_plan,
        RolloutMode::Shadow | RolloutMode::Canary { .. } | RolloutMode::RolledBack => {
            &decision.fallback_plan
        }
    }
}

fn new_rollout_current(
    evolution_id: &str,
    revision: u64,
    template_id: &str,
    decision: RolloutDecision,
) -> EvolutionRolloutCurrent {
    EvolutionRolloutCurrent {
        leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
        evolution_id: evolution_id.to_owned(),
        revision,
        template_id: template_id.to_owned(),
        decision,
    }
}

fn new_rollout_evidence_current(
    evolution_id: &str,
    revision: u64,
    template_id: &str,
    decision_id: &str,
) -> EvolutionResult<EvolutionRolloutEvidenceCurrent> {
    Ok(EvolutionRolloutEvidenceCurrent {
        leaf_version: EVOLUTION_STATE_LEAF_VERSION.to_owned(),
        evolution_id: evolution_id.to_owned(),
        revision,
        template_id: template_id.to_owned(),
        decision_id: decision_id.to_owned(),
        target_observations: 0,
        target_failures: 0,
        equivalent_shadows: 0,
        inequivalent_shadows: 0,
        evidence_count: 0,
        evidence_root: empty_evolution_evidence_root()?,
    })
}

fn validate_rollout_decision(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    template_id: &str,
    decision: &RolloutDecision,
) -> EvolutionResult<()> {
    validate_identity("rollout decision", &decision.decision_id)?;
    if let RolloutMode::Canary { basis_points } = decision.mode
        && basis_points > 10_000
    {
        return Err(EvolutionError::Validation(
            "canary basis_points must be <= 10000".to_owned(),
        ));
    }
    for plan_id in [&decision.fallback_plan, &decision.target_plan] {
        if plan_record(view, evolution_id, template_id, plan_id)?.is_none() {
            return Err(EvolutionError::NotFound(format!(
                "rollout Plan {plan_id} is missing"
            )));
        }
    }
    Ok(())
}

struct OccurrenceSelectionLineage {
    decision_id: String,
    plan: SealedPlan,
    existing_pin: Option<OccurrencePin>,
}

fn occurrence_selection_lineage(
    view: &EvolutionAuthorityView,
    evolution_id: &str,
    template_id: &str,
    occurrence_id: &str,
    selection_id: &str,
) -> EvolutionResult<OccurrenceSelectionLineage> {
    if template_current(view, evolution_id, template_id, "current")?.is_none() {
        return Err(EvolutionError::NotFound(format!(
            "live evolution template {template_id} is missing"
        )));
    }
    let selected = selection_current(view, evolution_id, template_id, selection_id)?;
    if let Some(existing) = occurrence_current(view, evolution_id, template_id, occurrence_id)? {
        if existing.pin.selection_id != selection_id {
            return Err(EvolutionError::Conflict(
                "occurrence is already pinned by a different deterministic selection".to_owned(),
            ));
        }
        if selected.is_none_or(|selection| {
            selection.occurrence_id != occurrence_id
                || selection.execution_binding != existing.pin.execution_binding
                || selection.decision_id != existing.pin.decision_id
                || selection.plan_id != existing.pin.plan_id
        }) {
            return Err(EvolutionError::Conflict(
                "retained occurrence is missing its exact selection alias".to_owned(),
            ));
        }
        let plan = plan_record(view, evolution_id, template_id, &existing.pin.plan_id)?
            .ok_or_else(|| {
                EvolutionError::NotFound("retained occurrence Plan is missing".to_owned())
            })?
            .plan
            .clone();
        return Ok(OccurrenceSelectionLineage {
            decision_id: existing.pin.decision_id.clone(),
            plan,
            existing_pin: Some(existing.pin.clone()),
        });
    }
    if selected.is_some() {
        return Err(EvolutionError::Conflict(
            "selection is already assigned to another occurrence".to_owned(),
        ));
    }
    let rollout = rollout_current(view, evolution_id, template_id, "current")?
        .ok_or_else(|| EvolutionError::NotFound("rollout current is missing".to_owned()))?;
    let plan_id = selected_plan(&rollout.decision, selection_id)?;
    let plan = plan_record(view, evolution_id, template_id, &plan_id)?
        .ok_or_else(|| EvolutionError::NotFound("selected occurrence Plan is missing".to_owned()))?
        .plan
        .clone();
    Ok(OccurrenceSelectionLineage {
        decision_id: rollout.decision.decision_id.clone(),
        plan,
        existing_pin: None,
    })
}

fn selected_plan(decision: &RolloutDecision, selection_id: &str) -> EvolutionResult<String> {
    Ok(match decision.mode {
        RolloutMode::Shadow | RolloutMode::RolledBack => decision.fallback_plan.clone(),
        RolloutMode::Active => decision.target_plan.clone(),
        RolloutMode::Canary { basis_points } => {
            let digest = content_id(
                CANARY_ID_DOMAIN,
                &(decision.decision_id.as_str(), selection_id),
            )?;
            let sample = u64::from_str_radix(&digest[7..23], 16)
                .map_err(|error| EvolutionError::Validation(error.to_string()))?;
            let bucket = u16::try_from((u128::from(sample) * 10_000) >> 64)
                .map_err(|error| EvolutionError::Validation(error.to_string()))?;
            if bucket < basis_points {
                decision.target_plan.clone()
            } else {
                decision.fallback_plan.clone()
            }
        }
    })
}

fn continuation_artifacts(
    continuation: &cymule_durable_protocol::Continuation,
) -> EvolutionResult<BTreeSet<ArtifactRef>> {
    let mut references = BTreeSet::new();
    if let Some(state) = &continuation.state {
        references.insert(state.clone());
    }
    if let Some(claim) = &continuation.execution_claim {
        references.insert(claim.execution_binding_ref.clone());
    }
    for frame in &continuation.frames {
        references.insert(frame.input.clone());
        references.extend(frame.locals.values().cloned());
    }
    for reference in &references {
        reference
            .validate()
            .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    }
    Ok(references)
}

fn shadow_matches_request(comparison: &ShadowComparison, request: &super::ShadowRequest) -> bool {
    comparison.comparison_id == request.comparison_id
        && comparison.subject == request.subject
        && comparison.decision_id == request.decision_id
        && comparison.primary_plan == request.primary_plan
        && comparison.shadow_plan == request.shadow_plan
        && comparison.driver_id == request.driver_id
        && comparison.driver_revision == request.driver_revision
        && comparison.comparison_policy == request.comparison_policy
}

fn advance_shadow_rollout(
    current: &EvolutionRolloutEvidenceCurrent,
    revision: u64,
    comparison: &ShadowComparison,
) -> EvolutionResult<EvolutionRolloutEvidenceCurrent> {
    if comparison.decision_id != current.decision_id {
        return Err(EvolutionError::Conflict(
            "shadow comparison does not match its rollout evidence current".to_owned(),
        ));
    }
    let mut next = current.clone();
    next.revision = revision;
    next.evidence_count = increment_exact(next.evidence_count, "rollout evidence count")?;
    next.evidence_root =
        advance_evolution_evidence_root(&next.evidence_root, &comparison.comparison_id)?;
    if comparison.equivalent {
        next.equivalent_shadows =
            increment_exact(next.equivalent_shadows, "equivalent shadow count")?;
    } else {
        next.inequivalent_shadows =
            increment_exact(next.inequivalent_shadows, "inequivalent shadow count")?;
    }
    Ok(next)
}

fn advance_observation_rollout(
    current: &EvolutionRolloutEvidenceCurrent,
    revision: u64,
    observation: &RolloutObservation,
) -> EvolutionResult<EvolutionRolloutEvidenceCurrent> {
    let mut next = current.clone();
    next.revision = revision;
    next.evidence_count = increment_exact(next.evidence_count, "rollout evidence count")?;
    next.evidence_root =
        advance_evolution_evidence_root(&next.evidence_root, &observation.observation_id)?;
    next.target_observations =
        increment_exact(next.target_observations, "target observation count")?;
    if observation.outcome == ObservationOutcome::Failed {
        next.target_failures = increment_exact(next.target_failures, "target failure count")?;
    }
    Ok(next)
}

fn increment_exact(value: u64, kind: &str) -> EvolutionResult<u64> {
    value
        .checked_add(1)
        .filter(|value| *value <= cymule_core::MAX_EXACT_INTEGER)
        .ok_or_else(|| EvolutionError::Validation(format!("{kind} exhausted the exact range")))
}

fn evaluate_rollout_gate(
    source_decision: &RolloutDecision,
    evidence: &EvolutionRolloutEvidenceCurrent,
    gate: &super::RolloutGate,
    next_decision_id: &str,
) -> EvolutionResult<(RolloutTransition, RolloutDecision)> {
    if gate.decision_id != source_decision.decision_id
        || evidence.decision_id != source_decision.decision_id
    {
        return Err(EvolutionError::Conflict(
            "rollout gate is stale relative to the exact current".to_owned(),
        ));
    }
    validate_identity("rollout gate", &gate.gate_id)?;
    validate_identity("rollout decision", next_decision_id)?;
    let outcome = if evidence.target_failures > gate.max_target_failures
        || evidence.inequivalent_shadows > gate.max_inequivalent_shadows
    {
        GateOutcome::Rollback
    } else if evidence.target_observations >= gate.min_target_observations
        && evidence.equivalent_shadows >= gate.min_equivalent_shadows
    {
        GateOutcome::Promote
    } else {
        return Err(EvolutionError::Conflict(
            "rollout gate requires more evidence".to_owned(),
        ));
    };
    let mut evaluation = RolloutEvaluation {
        evaluation_id: String::new(),
        gate: gate.clone(),
        target_observations: evidence.target_observations,
        target_failures: evidence.target_failures,
        equivalent_shadows: evidence.equivalent_shadows,
        inequivalent_shadows: evidence.inequivalent_shadows,
        outcome,
        evidence_count: evidence.evidence_count,
        evidence_root: evidence.evidence_root.clone(),
    };
    evaluation.evaluation_id = super::controller::derive_rollout_evaluation_id(&evaluation)?;
    let target_decision = RolloutDecision {
        decision_id: next_decision_id.to_owned(),
        fallback_plan: source_decision.fallback_plan.clone(),
        target_plan: source_decision.target_plan.clone(),
        mode: match outcome {
            GateOutcome::Promote => RolloutMode::Active,
            GateOutcome::Rollback => RolloutMode::RolledBack,
            GateOutcome::Pending => unreachable!("pending is rejected above"),
        },
    };
    let transition_id = super::controller::derive_rollout_transition_id(
        &source_decision.decision_id,
        &target_decision.decision_id,
        &evaluation,
    )?;
    Ok((
        RolloutTransition {
            transition_id,
            from_decision: source_decision.decision_id.clone(),
            to_decision: target_decision.decision_id.clone(),
            evaluation,
        },
        target_decision,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_core::{Expression, Operation, PlanCandidate, Region, Step};
    use serde_json::json;

    struct TestAuthority {
        current: Option<EvolutionCurrent>,
        leaves: BTreeMap<(EvolutionStateFamily, String), EvolutionMutation>,
    }

    impl TestAuthority {
        fn new() -> Self {
            Self {
                current: None,
                leaves: BTreeMap::new(),
            }
        }

        fn submit(&mut self, command: &EvolutionPersistenceCommand) -> EvolutionPostcondition {
            let mut view =
                EvolutionAuthorityView::new(command.evolution_id.clone(), self.current.clone())
                    .unwrap();
            for leaf in self.leaves.values().cloned() {
                view.insert(leaf).unwrap();
            }
            let source = EvolutionReductionSource::none();
            let prepared = loop {
                match prepare_evolution(&view, command, &source) {
                    Ok(prepared) => break prepared,
                    Err(EvolutionError::ReadRequired {
                        family,
                        storage_key,
                    }) => view.record_missing(family, storage_key).unwrap(),
                    Err(error) => panic!("unexpected prepare failure: {error}"),
                }
            };
            let provider =
                execute_evolution_provider(&prepared, &mut NoEvolutionProviders).unwrap();
            let postcondition = reduce_prepared_evolution(prepared, provider).unwrap();
            for mutation in postcondition.mutations.iter().cloned() {
                self.leaves
                    .insert(mutation.storage_key().unwrap(), mutation);
            }
            self.current = Some(postcondition.current.clone());
            postcondition
        }

        fn view(&self, evolution_id: &str) -> EvolutionAuthorityView {
            let mut view = EvolutionAuthorityView::new(evolution_id, self.current.clone()).unwrap();
            for leaf in self.leaves.values().cloned() {
                view.insert(leaf).unwrap();
            }
            view
        }
    }

    fn accounting_key(index: usize) -> (EvolutionStateFamily, String) {
        (
            EvolutionStateFamily::DefinitionRecord,
            content_id("cymule.test-evolution-source-key/1", &index).unwrap(),
        )
    }

    fn accounted_selection_source() -> EvolutionReductionSourceBody {
        let bytes = b"binding-source-accounting".to_vec();
        EvolutionReductionSourceBody::Selection {
            plan_id: content_id("cymule.test-plan/1", &"source-accounting").unwrap(),
            execution_binding: ArtifactRecord {
                reference: cymule_core::artifact_ref(
                    cymule_runtime::EXECUTION_BINDING_VERSION,
                    &bytes,
                )
                .unwrap(),
                bytes,
            },
        }
    }

    fn test_definition(id: &str, value: serde_json::Value) -> Definition {
        Definition {
            id: id.to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: Vec::new(),
                result: Expression::Literal { value },
            },
        }
    }

    fn review_definition(schema: serde_json::Value, value: serde_json::Value) -> Definition {
        Definition {
            id: "review".to_owned(),
            input_schema: schema.clone(),
            output_schema: schema,
            body: Region {
                steps: Vec::new(),
                result: Expression::Literal { value },
            },
        }
    }

    fn publish_review_definition(
        authority: &mut TestAuthority,
        command_id: &str,
        schema: serde_json::Value,
        value: serde_json::Value,
    ) -> EvolutionPostcondition {
        authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::PublishDefinition {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: command_id.to_owned(),
                    logical_ref: "review-flow".to_owned(),
                    definition: review_definition(schema, value),
                    references: Vec::new(),
                },
            )
            .unwrap(),
        )
    }

    fn publish_review_and_relink(
        authority: &mut TestAuthority,
        command_id: &str,
        schema: serde_json::Value,
        value: serde_json::Value,
    ) -> EvolutionPostcondition {
        authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::PublishAndRelink {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: command_id.to_owned(),
                    publication: super::super::LivePublicationCommand {
                        logical_ref: "review-flow".to_owned(),
                        definition: review_definition(schema, value),
                        references: Vec::new(),
                        evidence: publication_evidence(command_id),
                        mode: RolloutMode::Active,
                    },
                },
            )
            .unwrap(),
        )
    }

    fn published_revision(postcondition: &EvolutionPostcondition) -> SubflowRevision {
        let LiveEvolutionOutcome::DefinitionPublished { revision } = &postcondition.receipt.outcome
        else {
            panic!("definition publication returned the wrong outcome")
        };
        revision.clone()
    }

    fn review_template(string_schema: serde_json::Value) -> PlanTemplate {
        PlanTemplate {
            template_id: "template-review".to_owned(),
            candidate: PlanCandidate {
                ir_version: cymule_core::IR_VERSION.to_owned(),
                name: "review-parent".to_owned(),
                entry: "main".to_owned(),
                components: Vec::new(),
                effects: Vec::new(),
                definitions: vec![Definition {
                    id: "main".to_owned(),
                    input_schema: string_schema.clone(),
                    output_schema: string_schema.clone(),
                    body: Region {
                        steps: vec![Step {
                            id: "invoke-review".to_owned(),
                            operation: Operation::Invoke {
                                definition: "review-dependency".to_owned(),
                                input: Expression::Input,
                                bind: Some("reviewed".to_owned()),
                            },
                        }],
                        result: Expression::Binding {
                            name: "reviewed".to_owned(),
                        },
                    },
                }],
                metadata: BTreeMap::new(),
            },
            references: vec![SubflowReference::latest_compatible(
                "review-flow",
                "review-dependency",
                string_schema.clone(),
                string_schema,
            )],
        }
    }

    fn pinned_reference(logical_ref: &str, revision_id: String) -> SubflowReference {
        SubflowReference {
            logical_ref: logical_ref.to_owned(),
            local_definition: format!("dependency-{logical_ref}"),
            input_schema: json!({}),
            output_schema: json!({}),
            strategy: ReferenceStrategy::Pinned { revision_id },
        }
    }

    fn test_template(template_id: &str) -> PlanTemplate {
        PlanTemplate {
            template_id: template_id.to_owned(),
            candidate: PlanCandidate {
                ir_version: cymule_core::IR_VERSION.to_owned(),
                name: format!("{template_id}-plan"),
                entry: "main".to_owned(),
                components: Vec::new(),
                effects: Vec::new(),
                definitions: vec![test_definition("main", json!({"selected": true}))],
                metadata: BTreeMap::new(),
            },
            references: Vec::new(),
        }
    }

    fn test_execution_binding_record(plan: &SealedPlan) -> ArtifactRecord {
        let binding = cymule_runtime::ExecutionBinding {
            version: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
            context: cymule_runtime::BindingContextDescriptor {
                version: cymule_runtime::RUNTIME_COMPOSITION_VERSION.to_owned(),
                providers: Vec::new(),
            },
            components: BTreeMap::new(),
            effects: BTreeMap::new(),
        };
        binding.admit_plan(plan).unwrap();
        let bytes = binding.canonical_bytes().unwrap();
        ArtifactRecord {
            reference: cymule_core::artifact_ref(cymule_runtime::EXECUTION_BINDING_VERSION, &bytes)
                .unwrap(),
            bytes,
        }
    }

    #[test]
    fn many_small_entries_accept_exact_source_limit_and_reject_next_byte() {
        let mut view = EvolutionAuthorityView::new("evolution-main", None).unwrap();
        let available = MAX_EVOLUTION_SOURCE_BYTES - view.source_bytes();
        let entry_bytes = available / MAX_EVOLUTION_TRANSITION_LEAVES;
        let remainder = available % MAX_EVOLUTION_TRANSITION_LEAVES;
        for index in 0..MAX_EVOLUTION_TRANSITION_LEAVES {
            let bytes = entry_bytes + usize::from(index == 0) * remainder;
            view.replace_source_accounting(&accounting_key(index), bytes)
                .unwrap();
        }
        assert_eq!(view.source_bytes(), MAX_EVOLUTION_SOURCE_BYTES);

        let before_source_bytes = view.source_bytes;
        let before_accounted_entries = view.accounted_entries.clone();
        let error = view
            .replace_source_accounting(&accounting_key(MAX_EVOLUTION_TRANSITION_LEAVES), 1)
            .unwrap_err();
        assert!(matches!(error, EvolutionError::Validation(_)));
        assert_eq!(view.source_bytes, before_source_bytes);
        assert_eq!(view.accounted_entries, before_accounted_entries);
        assert!(verify_reduction_source_aggregate(&view, &accounted_selection_source()).is_err());
    }

    #[test]
    fn runtime_authority_is_included_in_the_exact_source_byte_boundary() {
        let source = accounted_selection_source();
        let source_bytes = match &source {
            EvolutionReductionSourceBody::Selection {
                plan_id,
                execution_binding,
            } => canonical_bytes(&(plan_id, execution_binding))
                .unwrap()
                .len(),
            _ => unreachable!(),
        };
        let mut view = EvolutionAuthorityView::new("evolution-main", None).unwrap();
        let remaining = MAX_EVOLUTION_SOURCE_BYTES - view.source_bytes() - source_bytes;
        view.replace_source_accounting(&accounting_key(0), remaining)
            .unwrap();
        verify_reduction_source_aggregate(&view, &source).unwrap();
        view.replace_source_accounting(&accounting_key(0), remaining + 1)
            .unwrap();
        assert!(verify_reduction_source_aggregate(&view, &source).is_err());
    }

    #[test]
    fn provider_preflight_exposes_exact_existing_artifacts_only_when_required() {
        let input = cymule_core::artifact_ref("cymule.test-shadow-input/1", b"input").unwrap();
        let command = EvolutionPersistenceCommand::new(
            "evolution-main",
            LiveEvolutionCommand::Apply {
                control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "live-shadow-1".to_owned(),
                template_id: "template-main".to_owned(),
                command: Box::new(EvolutionCommand::Shadow {
                    control_version: super::super::EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "shadow-1".to_owned(),
                    request: super::super::ShadowRequest {
                        comparison_id: "comparison-1".to_owned(),
                        decision_id: "decision-1".to_owned(),
                        subject: "subject-1".to_owned(),
                        primary_plan: content_id("cymule.test-plan/1", &"primary").unwrap(),
                        shadow_plan: content_id("cymule.test-plan/1", &"shadow").unwrap(),
                        input: input.clone(),
                        driver_id: "shadow-main".to_owned(),
                        driver_revision: content_id("cymule.test-shadow-driver/1", &()).unwrap(),
                        comparison_policy: "exact-output/1".to_owned(),
                    },
                }),
            },
        )
        .unwrap();
        let view = EvolutionAuthorityView::new("evolution-main", None).unwrap();
        let source = EvolutionReductionSource::none();
        let prepared = PreparedEvolution {
            view: &view,
            command: &command,
            revision: 1,
            parent_current_id: None,
            source: &source.body,
            source_witness_id: None,
            reduction: PreparedReduction::ProviderRequired,
        };
        assert_eq!(
            prepared.provider_required_artifacts().unwrap(),
            BTreeSet::from([input])
        );

        let deterministic = PreparedEvolution {
            reduction: PreparedReduction::Deterministic(Box::new(ReducedEvolution {
                outcome: LiveEvolutionOutcome::Applied,
                mutations: Vec::new(),
                plans: Vec::new(),
                artifacts: Vec::new(),
                required_artifacts: BTreeSet::new(),
            })),
            ..prepared
        };
        assert!(
            deterministic
                .provider_required_artifacts()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn maximum_sized_entries_accept_exact_source_limit_and_reject_one_more() {
        let mut view = EvolutionAuthorityView::new("evolution-main", None).unwrap();
        let available = MAX_EVOLUTION_SOURCE_BYTES - view.source_bytes();
        let maximum_entries = available / MAX_EVOLUTION_LEAF_BYTES;
        for index in 0..maximum_entries {
            view.replace_source_accounting(&accounting_key(index), MAX_EVOLUTION_LEAF_BYTES)
                .unwrap();
        }
        let remainder = available % MAX_EVOLUTION_LEAF_BYTES;
        assert!(remainder > 0);
        view.replace_source_accounting(&accounting_key(maximum_entries), remainder)
            .unwrap();
        assert_eq!(view.source_bytes(), MAX_EVOLUTION_SOURCE_BYTES);

        let error = view
            .replace_source_accounting(&accounting_key(maximum_entries + 1), 1)
            .unwrap_err();
        assert!(matches!(error, EvolutionError::Validation(_)));
        assert_eq!(view.source_bytes(), MAX_EVOLUTION_SOURCE_BYTES);
    }

    #[test]
    fn semantic_migration_wire_cannot_carry_durable_source_or_provider_product() {
        let exact = |domain: &str| content_id(domain, &()).unwrap();
        let command = EvolutionPersistenceCommand::new(
            "evolution-main",
            LiveEvolutionCommand::Apply {
                control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "live-migrate-1".to_owned(),
                template_id: "template-main".to_owned(),
                command: Box::new(EvolutionCommand::Migrate {
                    control_version: super::super::EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "migrate-1".to_owned(),
                    request: Box::new(super::super::MigrationRequest {
                        migration_id: "migration-1".to_owned(),
                        run_id: "run-1".to_owned(),
                        from_plan: exact("cymule.test-from-plan/1"),
                        to_plan: exact("cymule.test-to-plan/1"),
                        plan_edge_id: exact("cymule.test-plan-edge/1"),
                        compatibility_id: exact("cymule.test-compatibility/1"),
                        expected_source_epoch: 7,
                        adapter_id: "adapter-main".to_owned(),
                        adapter_revision: exact("cymule.test-adapter/1"),
                    }),
                }),
            },
        )
        .unwrap();
        let wire = serde_json::to_value(command).unwrap();
        let encoded = serde_json::to_string(&wire).unwrap();
        for forbidden in [
            "safe_point",
            "source_continuation",
            "source_binding",
            "target_binding",
            "input_state",
            "provider_product",
            "quiescence",
        ] {
            assert!(!encoded.contains(forbidden), "wire contains {forbidden}");
        }
        assert!(encoded.contains("adapter_id"));
        assert!(encoded.contains("expected_source_epoch"));
    }

    #[test]
    fn virtual_selection_derives_one_identity_and_retains_only_m1_binding_reference() {
        let mut authority = TestAuthority::new();
        let registered = authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::RegisterTemplate {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "register-template-main".to_owned(),
                    template: test_template("template-main"),
                },
            )
            .unwrap(),
        );
        let LiveEvolutionOutcome::TemplateRegistered { linked } = registered.receipt.outcome else {
            panic!("template registration returned the wrong outcome")
        };
        let execution_binding = test_execution_binding_record(&linked.plan);
        let typed_binding: cymule_runtime::ExecutionBinding =
            cymule_core::decode_json(&execution_binding.bytes).unwrap();
        assert_eq!(
            admit_evolution_target_binding(&linked.plan, &typed_binding).unwrap(),
            execution_binding
        );
        let run_execution = crate::virtual_work::VirtualRunExecution::Evolution {
            evolution_id: "evolution-main".to_owned(),
            template_id: "template-main".to_owned(),
        };
        let virtual_persistence_id =
            content_id("cymule.test-virtual-persistence/1", &"claim-1").unwrap();
        let expected_selection_id =
            derive_virtual_evolution_selection_id(&virtual_persistence_id).unwrap();
        let mut view = authority.view("evolution-main");
        let prepared = loop {
            match prepare_virtual_evolution_selection(
                &view,
                &run_execution,
                &virtual_persistence_id,
                "occurrence-1",
                &execution_binding.reference,
            ) {
                Ok(prepared) => break prepared,
                Err(EvolutionError::ReadRequired {
                    family,
                    storage_key,
                }) => view.record_missing(family, storage_key).unwrap(),
                Err(error) => panic!("unexpected selection preparation failure: {error}"),
            }
        };
        assert_eq!(prepared.selection_id(), expected_selection_id);
        assert_eq!(
            prepared.command().command.command_id(),
            expected_selection_id
        );
        assert_eq!(prepared.plan().plan_id, linked.plan.plan_id);
        assert_eq!(
            prepared.source_current().current_id,
            authority.current.as_ref().unwrap().current_id
        );
        let generic_command = prepared.command().clone();
        let wire = serde_json::to_string(prepared.command()).unwrap();
        assert!(wire.contains("execution_binding"));
        assert!(!wire.contains("bytes"));

        let mut tampered_binding = execution_binding.clone();
        tampered_binding.bytes.push(b' ');
        let error = reduce_evolution_selection(prepared, tampered_binding).unwrap_err();
        assert!(matches!(error, EvolutionError::Validation(_)));

        let prepared = prepare_evolution_selection(&view, &generic_command).unwrap();

        let postcondition =
            reduce_evolution_selection(prepared, execution_binding.clone()).unwrap();
        postcondition.verify().unwrap();
        assert!(postcondition.artifacts.is_empty());
        assert_eq!(
            postcondition.required_artifacts,
            BTreeSet::from([execution_binding.reference.clone()])
        );
        assert_eq!(postcondition.mutations.len(), 2);
        let LiveEvolutionOutcome::OccurrenceSelected { pin } = &postcondition.receipt.outcome
        else {
            panic!("cross-profile selection returned the wrong outcome")
        };
        assert_eq!(pin.selection_id, expected_selection_id);
        assert_eq!(pin.plan_id, linked.plan.plan_id);
        assert_eq!(pin.execution_binding, execution_binding.reference);
        assert_eq!(postcondition.current.revision, 2);
    }

    #[test]
    fn selection_rejects_duplicate_execution_binding_members() {
        let plan =
            cymule_core::seal_plan(test_template("template-duplicate-binding").candidate).unwrap();
        let binding = test_execution_binding_record(&plan);
        let encoded = String::from_utf8(binding.bytes).unwrap();
        let version = format!(
            "\"version\":\"{}\"",
            cymule_runtime::EXECUTION_BINDING_VERSION
        );
        let duplicate_bytes = encoded
            .replacen(&version, &format!("{version},{version}"), 1)
            .into_bytes();
        let duplicate_binding = ArtifactRecord {
            reference: cymule_core::artifact_ref(
                cymule_runtime::EXECUTION_BINDING_VERSION,
                &duplicate_bytes,
            )
            .unwrap(),
            bytes: duplicate_bytes,
        };
        assert!(EvolutionReductionSource::selection(&plan, duplicate_binding).is_err());
    }

    #[test]
    fn target_binding_admission_rejects_noncanonical_or_plan_incompatible_records() {
        let mut candidate = test_template("template-target-binding").candidate;
        candidate.components.push(cymule_core::ComponentContract {
            id: "required.component".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            output_artifact_kind: cymule_core::COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
            requirements: BTreeMap::new(),
        });
        let plan = cymule_core::seal_plan(candidate).unwrap();
        let empty_binding = cymule_runtime::ExecutionBinding {
            version: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
            context: cymule_runtime::BindingContextDescriptor {
                version: cymule_runtime::RUNTIME_COMPOSITION_VERSION.to_owned(),
                providers: Vec::new(),
            },
            components: BTreeMap::new(),
            effects: BTreeMap::new(),
        };
        empty_binding.verify().unwrap();
        let bytes = empty_binding.canonical_bytes().unwrap();
        let record = ArtifactRecord {
            reference: cymule_core::artifact_ref(cymule_runtime::EXECUTION_BINDING_VERSION, &bytes)
                .unwrap(),
            bytes,
        };
        assert!(matches!(
            verify_evolution_target_binding_record(&plan, &record),
            Err(EvolutionError::Conflict(_))
        ));
        assert!(matches!(
            admit_evolution_target_binding(&plan, &empty_binding),
            Err(EvolutionError::Conflict(_))
        ));

        let empty_plan =
            cymule_core::seal_plan(test_template("template-canonical-binding").candidate).unwrap();
        let mut noncanonical_bytes = empty_binding.canonical_bytes().unwrap();
        noncanonical_bytes.push(b'\n');
        let noncanonical = ArtifactRecord {
            reference: cymule_core::artifact_ref(
                cymule_runtime::EXECUTION_BINDING_VERSION,
                &noncanonical_bytes,
            )
            .unwrap(),
            bytes: noncanonical_bytes,
        };
        assert!(matches!(
            verify_evolution_target_binding_record(&empty_plan, &noncanonical),
            Err(EvolutionError::Validation(_))
        ));
    }

    #[test]
    fn virtual_selection_fails_closed_before_binding_when_exact_plan_state_is_missing() {
        let receipt_id = content_id("cymule.test-evolution-receipt/1", &()).unwrap();
        let current = EvolutionCurrent {
            current_version: EVOLUTION_CURRENT_VERSION.to_owned(),
            current_id: content_id(
                EVOLUTION_CURRENT_VERSION,
                &(
                    EVOLUTION_CURRENT_VERSION,
                    "evolution-main",
                    1_u64,
                    receipt_id.as_str(),
                ),
            )
            .unwrap(),
            evolution_id: "evolution-main".to_owned(),
            revision: 1,
            last_receipt_id: receipt_id,
        };
        let view = EvolutionAuthorityView::new("evolution-main", Some(current)).unwrap();
        let execution_binding = cymule_core::artifact_ref(
            cymule_runtime::EXECUTION_BINDING_VERSION,
            b"already-admitted-binding",
        )
        .unwrap();
        let run_execution = crate::virtual_work::VirtualRunExecution::Evolution {
            evolution_id: "evolution-main".to_owned(),
            template_id: "template-main".to_owned(),
        };
        let error = prepare_virtual_evolution_selection(
            &view,
            &run_execution,
            &content_id("cymule.test-virtual-persistence/1", &"claim-1").unwrap(),
            "occurrence-1",
            &execution_binding,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            EvolutionError::ReadRequired {
                family: EvolutionStateFamily::TemplateCurrent,
                ..
            }
        ));
    }

    #[test]
    fn direct_virtual_run_has_no_m4_selection_path() {
        let view = EvolutionAuthorityView::new("evolution-main", None).unwrap();
        let execution_binding = cymule_core::artifact_ref(
            cymule_runtime::EXECUTION_BINDING_VERSION,
            b"already-admitted-binding",
        )
        .unwrap();
        let direct = crate::virtual_work::VirtualRunExecution::Direct {
            plan_id: content_id("cymule.test-plan/1", &"direct").unwrap(),
        };
        let error = prepare_virtual_evolution_selection(
            &view,
            &direct,
            &content_id("cymule.test-virtual-persistence/1", &"claim-1").unwrap(),
            "occurrence-1",
            &execution_binding,
        )
        .unwrap_err();
        assert!(matches!(error, EvolutionError::Conflict(_)));
        assert!(view.is_empty());
    }

    #[test]
    fn canary_selection_boundaries_are_total_and_deterministic() {
        let decision = |basis_points| RolloutDecision {
            decision_id: "decision-canary".to_owned(),
            fallback_plan: content_id("cymule.test-plan/1", &"fallback").unwrap(),
            target_plan: content_id("cymule.test-plan/1", &"target").unwrap(),
            mode: RolloutMode::Canary { basis_points },
        };
        let selection_id = content_id("cymule.test-selection/1", &()).unwrap();
        assert_eq!(
            selected_plan(&decision(0), &selection_id).unwrap(),
            decision(0).fallback_plan
        );
        assert_eq!(
            selected_plan(&decision(10_000), &selection_id).unwrap(),
            decision(10_000).target_plan
        );
        assert_eq!(
            selected_plan(&decision(5_000), &selection_id).unwrap(),
            selected_plan(&decision(5_000), &selection_id).unwrap()
        );
    }

    #[test]
    fn publication_references_are_required_exact_ordered_and_receipted() {
        let mut authority = TestAuthority::new();
        let dependency = authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::PublishDefinition {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "publish-dependency".to_owned(),
                    logical_ref: "dependency".to_owned(),
                    definition: test_definition("dependency", json!("dependency")),
                    references: Vec::new(),
                },
            )
            .unwrap(),
        );
        let LiveEvolutionOutcome::DefinitionPublished {
            revision: dependency,
        } = dependency.receipt.outcome
        else {
            panic!("dependency publication returned the wrong outcome")
        };
        let references = vec![pinned_reference(
            "dependency",
            dependency.revision_id.clone(),
        )];
        let command = EvolutionPersistenceCommand::new(
            "evolution-main",
            LiveEvolutionCommand::PublishDefinition {
                control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "publish-parent".to_owned(),
                logical_ref: "parent".to_owned(),
                definition: test_definition("parent", json!("parent")),
                references: references.clone(),
            },
        )
        .unwrap();
        let mut wire = serde_json::to_value(&command.command).unwrap();
        wire.as_object_mut().unwrap().remove("references");
        assert!(serde_json::from_value::<LiveEvolutionCommand>(wire).is_err());

        let published = authority.submit(&command);
        let LiveEvolutionOutcome::DefinitionPublished { revision } = published.receipt.outcome
        else {
            panic!("parent publication returned the wrong outcome")
        };
        assert_eq!(revision.references, references);
    }

    #[test]
    fn publication_references_reject_non_exact_order_and_bounds() {
        let revision_a = content_id("cymule.test-subflow/1", &"a").unwrap();
        let revision_b = content_id("cymule.test-subflow/1", &"b").unwrap();
        let command = |references| {
            EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::PublishDefinition {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "publish-parent".to_owned(),
                    logical_ref: "parent".to_owned(),
                    definition: test_definition("parent", json!("parent")),
                    references,
                },
            )
        };
        assert!(
            command(vec![
                pinned_reference("b", revision_b),
                pinned_reference("a", revision_a),
            ])
            .is_err()
        );
        assert!(
            command(vec![SubflowReference::latest_compatible(
                "a",
                "dependency-a",
                json!({}),
                json!({}),
            )])
            .is_err()
        );
        let too_many = (0..=super::super::MAX_SUBFLOW_REFERENCES)
            .map(|index| {
                pinned_reference(
                    &format!("dependency-{index:04}"),
                    content_id("cymule.test-subflow/1", &index).unwrap(),
                )
            })
            .collect();
        assert!(command(too_many).is_err());
        let oversized_schema = json!({
            "description": "x".repeat(super::super::MAX_SUBFLOW_REFERENCE_BYTES)
        });
        assert!(
            command(vec![SubflowReference {
                logical_ref: "oversized".to_owned(),
                local_definition: "dependency-oversized".to_owned(),
                input_schema: oversized_schema,
                output_schema: json!({}),
                strategy: ReferenceStrategy::Pinned {
                    revision_id: content_id("cymule.test-subflow/1", &"oversized").unwrap(),
                },
            }])
            .is_err()
        );
    }

    #[test]
    fn missing_pinned_publication_dependency_fails_during_pure_prepare() {
        let command = EvolutionPersistenceCommand::new(
            "evolution-main",
            LiveEvolutionCommand::PublishDefinition {
                control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "publish-parent".to_owned(),
                logical_ref: "parent".to_owned(),
                definition: test_definition("parent", json!("parent")),
                references: vec![pinned_reference(
                    "missing",
                    content_id("cymule.test-subflow/1", &"missing").unwrap(),
                )],
            },
        )
        .unwrap();
        let source = EvolutionReductionSource::none();
        let mut view = EvolutionAuthorityView::new("evolution-main", None).unwrap();
        loop {
            match prepare_evolution(&view, &command, &source) {
                Err(EvolutionError::ReadRequired {
                    family,
                    storage_key,
                }) => view.record_missing(family, storage_key).unwrap(),
                Err(EvolutionError::NotFound(message)) => {
                    assert!(message.contains("pinned subflow revision"));
                    break;
                }
                Ok(_) => panic!("missing pinned revision reached provider staging"),
                Err(error) => panic!("unexpected prepare failure: {error}"),
            }
        }
    }

    fn publish_named_definition(
        authority: &mut TestAuthority,
        command_id: &str,
        logical_ref: &str,
        definition: Definition,
        references: Vec<SubflowReference>,
    ) -> EvolutionPostcondition {
        authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::PublishDefinition {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: command_id.to_owned(),
                    logical_ref: logical_ref.to_owned(),
                    definition,
                    references,
                },
            )
            .unwrap(),
        )
    }

    fn transitive_middle_definition(
        schema: &serde_json::Value,
        pinned_leaf: &SubflowReference,
    ) -> Definition {
        Definition {
            id: "middle".to_owned(),
            input_schema: schema.clone(),
            output_schema: schema.clone(),
            body: Region {
                steps: vec![Step {
                    id: "invoke-leaf".to_owned(),
                    operation: Operation::Invoke {
                        definition: pinned_leaf.local_definition.clone(),
                        input: Expression::Input,
                        bind: Some("leaf-result".to_owned()),
                    },
                }],
                result: Expression::Binding {
                    name: "leaf-result".to_owned(),
                },
            },
        }
    }

    fn transitive_pin_template(
        schema: &serde_json::Value,
        middle_reference: SubflowReference,
    ) -> PlanTemplate {
        PlanTemplate {
            template_id: "template-transitive-pin".to_owned(),
            candidate: PlanCandidate {
                ir_version: cymule_core::IR_VERSION.to_owned(),
                name: "transitive-pin-parent".to_owned(),
                entry: "main".to_owned(),
                components: Vec::new(),
                effects: Vec::new(),
                definitions: vec![Definition {
                    id: "main".to_owned(),
                    input_schema: schema.clone(),
                    output_schema: schema.clone(),
                    body: Region {
                        steps: vec![Step {
                            id: "invoke-middle".to_owned(),
                            operation: Operation::Invoke {
                                definition: middle_reference.local_definition.clone(),
                                input: Expression::Input,
                                bind: Some("middle-result".to_owned()),
                            },
                        }],
                        result: Expression::Binding {
                            name: "middle-result".to_owned(),
                        },
                    },
                }],
                metadata: BTreeMap::new(),
            },
            references: vec![middle_reference],
        }
    }

    fn publication_evidence(value: &str) -> ArtifactRecord {
        let bytes = value.as_bytes().to_vec();
        ArtifactRecord {
            reference: cymule_core::artifact_ref("cymule.test-publication-evidence/1", &bytes)
                .unwrap(),
            bytes,
        }
    }

    struct PinnedClosureFixture {
        authority: TestAuthority,
        middle_definition: Definition,
        original_plan: String,
    }

    fn pinned_closure_fixture() -> PinnedClosureFixture {
        let schema = json!({});
        let mut authority = TestAuthority::new();
        let first_leaf = publish_named_definition(
            &mut authority,
            "publish-leaf-1",
            "flow-a",
            test_definition("leaf", json!("a1")),
            Vec::new(),
        );
        let first_leaf = published_revision(&first_leaf);
        let pinned_leaf = pinned_reference("flow-a", first_leaf.revision_id);
        let middle_definition = transitive_middle_definition(&schema, &pinned_leaf);
        publish_named_definition(
            &mut authority,
            "publish-middle-1",
            "flow-b",
            middle_definition.clone(),
            vec![pinned_leaf],
        );
        let middle_reference = SubflowReference::latest_compatible(
            "flow-b",
            "dependency-flow-b",
            schema.clone(),
            schema.clone(),
        );
        let registered = authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::RegisterTemplate {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "register-transitive-pin".to_owned(),
                    template: transitive_pin_template(&schema, middle_reference),
                },
            )
            .unwrap(),
        );
        let LiveEvolutionOutcome::TemplateRegistered { linked } = &registered.receipt.outcome
        else {
            panic!("template registration returned the wrong outcome")
        };
        PinnedClosureFixture {
            authority,
            middle_definition,
            original_plan: linked.plan.plan_id.clone(),
        }
    }

    fn publish_new_pinned_leaf(fixture: &mut PinnedClosureFixture) -> SubflowRevision {
        let publication = fixture.authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::PublishAndRelink {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "publish-leaf-2".to_owned(),
                    publication: super::super::LivePublicationCommand {
                        logical_ref: "flow-a".to_owned(),
                        definition: test_definition("leaf", json!("a2")),
                        references: Vec::new(),
                        evidence: publication_evidence("leaf-2"),
                        mode: RolloutMode::Active,
                    },
                },
            )
            .unwrap(),
        );
        let LiveEvolutionOutcome::PublicationApplied { receipt } = &publication.receipt.outcome
        else {
            panic!("leaf publication returned the wrong outcome")
        };
        assert!(receipt.updates.is_empty());
        let view = fixture.authority.view("evolution-main");
        let current = template_current(
            &view,
            "evolution-main",
            "template-transitive-pin",
            "current",
        )
        .unwrap()
        .unwrap();
        assert_eq!(current.linked.plan.plan_id, fixture.original_plan);
        receipt.revision.clone()
    }

    fn publish_new_direct_dependency(
        fixture: &mut PinnedClosureFixture,
        leaf_revision: &SubflowRevision,
    ) {
        let publication = fixture.authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::PublishAndRelink {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "publish-middle-2".to_owned(),
                    publication: super::super::LivePublicationCommand {
                        logical_ref: "flow-b".to_owned(),
                        definition: fixture.middle_definition.clone(),
                        references: vec![pinned_reference(
                            "flow-a",
                            leaf_revision.revision_id.clone(),
                        )],
                        evidence: publication_evidence("middle-2"),
                        mode: RolloutMode::Active,
                    },
                },
            )
            .unwrap(),
        );
        let LiveEvolutionOutcome::PublicationApplied { receipt } = &publication.receipt.outcome
        else {
            panic!("middle publication returned the wrong outcome")
        };
        assert_eq!(receipt.updates.len(), 1);
        assert_eq!(receipt.updates[0].template_id, "template-transitive-pin");
        assert!(receipt.updates[0].advanced);
        assert_eq!(receipt.updates[0].previous_plan_id, fixture.original_plan);
        assert_ne!(receipt.updates[0].current_plan_id, fixture.original_plan);
        assert_eq!(
            receipt.revision.references[0].strategy,
            ReferenceStrategy::Pinned {
                revision_id: leaf_revision.revision_id.clone(),
            }
        );
    }

    #[test]
    fn pinned_definition_closure_changes_only_after_its_direct_latest_head_advances() {
        let mut fixture = pinned_closure_fixture();
        let second_leaf = publish_new_pinned_leaf(&mut fixture);
        publish_new_direct_dependency(&mut fixture, &second_leaf);
    }

    #[test]
    fn genesis_receipt_has_no_result_manifest_or_current_cycle() {
        let command = EvolutionPersistenceCommand::new(
            "evolution-main",
            LiveEvolutionCommand::PublishDefinition {
                control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "publish-definition-1".to_owned(),
                logical_ref: "review-flow".to_owned(),
                definition: Definition {
                    id: "review".to_owned(),
                    input_schema: json!({}),
                    output_schema: json!({}),
                    body: Region {
                        steps: Vec::new(),
                        result: Expression::Literal {
                            value: json!({"result": "ok"}),
                        },
                    },
                },
                references: Vec::new(),
            },
        )
        .unwrap();
        let source = EvolutionReductionSource::none();
        let mut view = EvolutionAuthorityView::new("evolution-main", None).unwrap();
        let prepared = loop {
            match prepare_evolution(&view, &command, &source) {
                Ok(prepared) => break prepared,
                Err(EvolutionError::ReadRequired {
                    family,
                    storage_key,
                }) => view.record_missing(family, storage_key).unwrap(),
                Err(error) => panic!("unexpected prepare failure: {error}"),
            }
        };
        let provider = execute_evolution_provider(&prepared, &mut NoEvolutionProviders).unwrap();
        let postcondition = reduce_prepared_evolution(prepared, provider).unwrap();
        postcondition.verify().unwrap();

        assert_eq!(postcondition.current.revision, 1);
        assert_eq!(
            postcondition.current.last_receipt_id,
            postcondition.receipt.receipt_id
        );
        assert!(postcondition.receipt.parent_current_id.is_none());
        let receipt = serde_json::to_value(&postcondition.receipt).unwrap();
        let receipt = receipt.as_object().unwrap();
        assert_eq!(
            receipt
                .get("mutations")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(3)
        );
        for forbidden in [
            "current_id",
            "result_revision",
            "state_root",
            "manifest",
            "cas_token",
        ] {
            assert!(!receipt.contains_key(forbidden));
        }
        let mut missing_mutations = serde_json::to_value(&postcondition.receipt).unwrap();
        missing_mutations
            .as_object_mut()
            .unwrap()
            .remove("mutations");
        assert!(serde_json::from_value::<EvolutionPersistenceReceipt>(missing_mutations).is_err());

        let observed_revision = content_id("cymule.test-state-root/1", &()).unwrap();
        let replay = EvolutionCommit {
            observed_revision: observed_revision.clone(),
            committed_revision: None,
            receipt: postcondition.receipt.clone(),
        };
        replay.verify_for(&command).unwrap();
        let mut wire = serde_json::to_value(&replay).unwrap();
        assert!(
            wire.get("committed_revision")
                .is_some_and(serde_json::Value::is_null)
        );
        wire.as_object_mut().unwrap().remove("committed_revision");
        assert!(serde_json::from_value::<EvolutionCommit>(wire).is_err());

        let first = EvolutionCommit {
            observed_revision: observed_revision.clone(),
            committed_revision: Some(observed_revision),
            receipt: postcondition.receipt,
        };
        first.verify_for(&command).unwrap();
    }

    #[test]
    fn receipt_rejects_a_source_witness_that_differs_from_its_semantic_outcome() {
        let target_plan =
            cymule_core::seal_plan(test_template("restart-target").candidate).unwrap();
        let artifact =
            |kind: &str, value: &str| cymule_core::artifact_ref(kind, value.as_bytes()).unwrap();
        let source_witness = content_id("cymule.test-source-witness/1", &"source-a").unwrap();
        let request = super::super::RestartRequest {
            restart_id: "restart-1".to_owned(),
            replacement_run: "replacement-run-1".to_owned(),
            run_id: "source-run-1".to_owned(),
            from_plan: content_id("cymule.test-source-plan/1", &()).unwrap(),
            expected_source_epoch: 7,
            to_plan: target_plan.plan_id.clone(),
            input: artifact("cymule.test-input/1", "input"),
            evidence: artifact("cymule.test-evidence/1", "evidence"),
        };
        let command = EvolutionPersistenceCommand::new(
            "evolution-main",
            LiveEvolutionCommand::Apply {
                control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "live-restart-1".to_owned(),
                template_id: "template-main".to_owned(),
                command: Box::new(EvolutionCommand::RestartUnderNewPlan {
                    control_version: super::super::EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "restart-command-1".to_owned(),
                    request: Box::new(request.clone()),
                }),
            },
        )
        .unwrap();
        let mutations = Vec::<EvolutionMutationWrite>::new();
        let mutation_id = content_id(EVOLUTION_MUTATION_SET_VERSION, &mutations).unwrap();
        let mut receipt = EvolutionPersistenceReceipt {
            receipt_version: EVOLUTION_PERSISTENCE_RECEIPT_VERSION.to_owned(),
            receipt_id: String::new(),
            command,
            parent_current_id: None,
            source_witness_id: Some(source_witness.clone()),
            outcome: LiveEvolutionOutcome::RestartAuthorized {
                receipt: Box::new(super::super::RestartReceipt {
                    request,
                    source_witness_id: source_witness,
                    target_plan,
                }),
            },
            mutations,
            mutation_id,
        };
        receipt.receipt_id = receipt.derived_id().unwrap();
        receipt.verify().unwrap();

        receipt.source_witness_id =
            Some(content_id("cymule.test-source-witness/1", &"source-b").unwrap());
        receipt.receipt_id = receipt.derived_id().unwrap();
        let error = receipt.verify().unwrap_err();
        assert!(
            matches!(error, EvolutionError::Validation(message) if message.contains("semantic outcome"))
        );
    }

    #[test]
    fn latest_compatible_uses_exact_contract_current_not_incompatible_global_head() {
        let string_schema = json!({"type": "string"});
        let number_schema = json!({"type": "number"});
        let mut authority = TestAuthority::new();
        let string = publish_review_definition(
            &mut authority,
            "publish-string",
            string_schema.clone(),
            json!("one"),
        );
        let string_revision = published_revision(&string);
        let duplicate = publish_review_definition(
            &mut authority,
            "publish-string-again",
            string_schema.clone(),
            json!("one"),
        );
        let duplicate_revision = published_revision(&duplicate);
        assert_eq!(duplicate_revision, string_revision);
        assert!(duplicate.mutations.is_empty());
        let number =
            publish_review_definition(&mut authority, "publish-number", number_schema, json!(2));
        let number_revision = published_revision(&number);
        assert_ne!(string_revision.revision_id, number_revision.revision_id);
        assert_eq!(number_revision.sequence, string_revision.sequence + 1);

        let registered = authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::RegisterTemplate {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "register-review-template".to_owned(),
                    template: review_template(string_schema),
                },
            )
            .unwrap(),
        );
        let LiveEvolutionOutcome::TemplateRegistered { linked } = registered.receipt.outcome else {
            panic!("template registration returned the wrong outcome")
        };
        assert_eq!(
            linked.resolved_revisions.get("review-flow"),
            Some(&string_revision.revision_id)
        );
    }

    #[test]
    fn republishing_historical_content_reuses_immutable_revision_and_moves_only_heads() {
        let schema = json!({"type": "string"});
        let mut authority = TestAuthority::new();
        let first = publish_review_definition(
            &mut authority,
            "publish-first",
            schema.clone(),
            json!("first"),
        );
        let first_revision = published_revision(&first);
        let second = publish_review_definition(
            &mut authority,
            "publish-second",
            schema.clone(),
            json!("second"),
        );
        assert_ne!(
            published_revision(&second).revision_id,
            first_revision.revision_id
        );

        let restored = publish_review_definition(
            &mut authority,
            "publish-first-again",
            schema.clone(),
            json!("first"),
        );
        assert_eq!(published_revision(&restored), first_revision);
        assert_eq!(restored.mutations.len(), 2);
        assert!(restored.mutations.iter().all(|mutation| matches!(
            mutation,
            EvolutionMutation::DefinitionCurrent(_)
                | EvolutionMutation::DefinitionCompatibilityCurrent(_)
        )));

        let restored_current = restored
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                EvolutionMutation::DefinitionCurrent(current) => Some(current.as_ref()),
                _ => None,
            })
            .unwrap();
        assert_eq!(restored_current.latest.sequence, first_revision.sequence);
        assert_eq!(restored_current.max_sequence, 2);

        let replayed_head = publish_review_definition(
            &mut authority,
            "publish-first-head-again",
            schema.clone(),
            json!("first"),
        );
        assert_eq!(published_revision(&replayed_head), first_revision);
        assert!(replayed_head.mutations.is_empty());
        replayed_head.receipt.verify().unwrap();

        let third = publish_review_definition(
            &mut authority,
            "publish-third-after-restore",
            schema,
            json!("third"),
        );
        assert_eq!(published_revision(&third).sequence, 3);
    }

    #[test]
    fn definition_sequence_allocator_fails_closed_at_the_exact_integer_limit() {
        let schema = json!({"type": "string"});
        let mut authority = TestAuthority::new();
        let first = publish_review_definition(
            &mut authority,
            "publish-before-exhaustion",
            schema.clone(),
            json!("first"),
        );
        let first_revision = published_revision(&first);
        publish_review_definition(
            &mut authority,
            "publish-second-before-exhaustion",
            schema.clone(),
            json!("second"),
        );
        let current = authority
            .leaves
            .values_mut()
            .find_map(|mutation| match mutation {
                EvolutionMutation::DefinitionCurrent(current)
                    if current.logical_ref == "review-flow" =>
                {
                    Some(current.as_mut())
                }
                _ => None,
            })
            .unwrap();
        current.max_sequence = cymule_core::MAX_EXACT_INTEGER;
        verify_definition_leaf(current, false).unwrap();

        let restored = publish_review_definition(
            &mut authority,
            "restore-historical-after-exhaustion",
            schema.clone(),
            json!("first"),
        );
        assert_eq!(published_revision(&restored), first_revision);

        let mut view = authority.view("evolution-main");
        let error = loop {
            match prepare_definition_publication(
                &view,
                "evolution-main",
                "review-flow",
                &review_definition(schema.clone(), json!("next")),
                &[],
            ) {
                Err(EvolutionError::ReadRequired {
                    family,
                    storage_key,
                }) => view.record_missing(family, storage_key).unwrap(),
                Err(error) => break error,
                Ok(_) => panic!("exhausted definition sequence allocator accepted a new revision"),
            }
        };
        assert!(
            matches!(error, EvolutionError::Validation(message) if message.contains("sequence exhausted"))
        );
    }

    struct HistoricalRelinkFixture {
        schema: serde_json::Value,
        authority: TestAuthority,
        first_plan: String,
        second_plan: String,
        second_decision: String,
        first_edge: EvolutionEdgeRecord,
        advanced: EvolutionPostcondition,
        second_evidence: EvolutionRolloutEvidenceCurrent,
    }

    struct RestoredRelink {
        postcondition: EvolutionPostcondition,
        decision_id: String,
        edge: EvolutionEdgeRecord,
    }

    fn historical_relink_fixture() -> HistoricalRelinkFixture {
        let schema = json!({"type": "string"});
        let mut authority = TestAuthority::new();
        publish_review_definition(
            &mut authority,
            "publish-review-1",
            schema.clone(),
            json!("first"),
        );
        let registered = authority.submit(
            &EvolutionPersistenceCommand::new(
                "evolution-main",
                LiveEvolutionCommand::RegisterTemplate {
                    control_version: super::super::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "register-review".to_owned(),
                    template: review_template(schema.clone()),
                },
            )
            .unwrap(),
        );
        let LiveEvolutionOutcome::TemplateRegistered { linked } = &registered.receipt.outcome
        else {
            panic!("review template registration returned the wrong outcome")
        };
        let first_plan = linked.plan.plan_id.clone();
        let initial_decision = rollout_current(
            &authority.view("evolution-main"),
            "evolution-main",
            "template-review",
            "current",
        )
        .unwrap()
        .unwrap()
        .decision
        .decision_id
        .clone();

        let advanced_postcondition = publish_review_and_relink(
            &mut authority,
            "publish-review-2",
            schema.clone(),
            json!("second"),
        );
        let LiveEvolutionOutcome::PublicationApplied { receipt: advanced } =
            &advanced_postcondition.receipt.outcome
        else {
            panic!("review publication returned the wrong outcome")
        };
        assert_eq!(advanced.updates.len(), 1);
        let second_plan = advanced.updates[0].current_plan_id.clone();
        let second_decision = advanced.updates[0].decision_id.clone().unwrap();
        assert_ne!(second_plan, first_plan);
        assert_eq!(
            second_decision,
            update_decision(
                "template-review",
                &initial_decision,
                &first_plan,
                &second_plan,
                RolloutMode::Active,
            )
            .unwrap()
            .decision_id
        );
        let first_edge = advanced_postcondition
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                EvolutionMutation::EdgeRecord(record) => Some(record.as_ref().clone()),
                _ => None,
            })
            .expect("first relink retains one immutable edge");
        let first_publication_evidence = publication_evidence("publish-review-2");
        assert_eq!(first_edge.evidence, first_publication_evidence.reference);
        assert_eq!(
            advanced_postcondition.artifacts,
            vec![first_publication_evidence]
        );
        assert_edge_record_tamper_is_rejected(&advanced_postcondition, &first_edge);
        let second_evidence = rollout_evidence_current(
            &authority.view("evolution-main"),
            "evolution-main",
            "template-review",
            &second_decision,
        )
        .unwrap()
        .unwrap()
        .clone();
        HistoricalRelinkFixture {
            schema,
            authority,
            first_plan,
            second_plan,
            second_decision,
            first_edge,
            advanced: advanced_postcondition,
            second_evidence,
        }
    }

    fn assert_edge_record_tamper_is_rejected(
        advanced: &EvolutionPostcondition,
        first_edge: &EvolutionEdgeRecord,
    ) {
        let mut missing_evidence =
            serde_json::to_value(EvolutionMutation::EdgeRecord(Box::new(first_edge.clone())))
                .unwrap();
        missing_evidence.as_object_mut().unwrap().remove("evidence");
        assert!(serde_json::from_value::<EvolutionMutation>(missing_evidence).is_err());
        let mut edge_field_tamper = advanced.clone();
        let tampered_edge = edge_field_tamper
            .mutations
            .iter_mut()
            .find_map(|mutation| match mutation {
                EvolutionMutation::EdgeRecord(record) => Some(record.as_mut()),
                _ => None,
            })
            .unwrap();
        tampered_edge.edge.to_plan = content_id("cymule.test-plan/1", &"tampered").unwrap();
        assert!(edge_field_tamper.verify().is_err());

        let mut evidence_tamper = advanced.clone();
        let tampered_record = evidence_tamper
            .mutations
            .iter_mut()
            .find_map(|mutation| match mutation {
                EvolutionMutation::EdgeRecord(record) => Some(record.as_mut()),
                _ => None,
            })
            .unwrap();
        tampered_record.evidence = publication_evidence("tampered-evidence").reference;
        assert!(evidence_tamper.verify().is_err());

        let mut missing_artifact = advanced.clone();
        missing_artifact.artifacts.clear();
        assert!(missing_artifact.verify().is_err());
    }

    fn restore_historical_relink(fixture: &mut HistoricalRelinkFixture) -> RestoredRelink {
        let restored_postcondition = publish_review_and_relink(
            &mut fixture.authority,
            "restore-review-1",
            fixture.schema.clone(),
            json!("first"),
        );
        let LiveEvolutionOutcome::PublicationApplied {
            receipt: restored_receipt,
        } = &restored_postcondition.receipt.outcome
        else {
            panic!("historical review publication returned the wrong outcome")
        };
        assert_eq!(restored_receipt.updates.len(), 1);
        assert_eq!(
            restored_receipt.updates[0].previous_plan_id,
            fixture.second_plan
        );
        assert_eq!(
            restored_receipt.updates[0].current_plan_id,
            fixture.first_plan
        );
        assert!(restored_receipt.updates[0].advanced);
        let restored_decision = restored_receipt.updates[0].decision_id.clone().unwrap();
        assert_eq!(
            restored_decision,
            update_decision(
                "template-review",
                &fixture.second_decision,
                &fixture.second_plan,
                &fixture.first_plan,
                RolloutMode::Active,
            )
            .unwrap()
            .decision_id
        );
        assert!(
            !restored_postcondition
                .mutations
                .iter()
                .any(|mutation| matches!(mutation, EvolutionMutation::PlanRecord(_)))
        );
        assert!(
            !restored_postcondition
                .mutations
                .iter()
                .any(|mutation| matches!(mutation, EvolutionMutation::LinkRecord(_)))
        );
        let reverse_edge = restored_postcondition
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                EvolutionMutation::EdgeRecord(record) => Some(record.as_ref().clone()),
                _ => None,
            })
            .expect("the first reverse transition retains one immutable edge");
        let restored_publication_evidence = publication_evidence("restore-review-1");
        assert_eq!(
            reverse_edge.evidence,
            restored_publication_evidence.reference
        );
        assert_eq!(
            restored_postcondition.artifacts,
            vec![restored_publication_evidence]
        );
        RestoredRelink {
            postcondition: restored_postcondition,
            decision_id: restored_decision,
            edge: reverse_edge,
        }
    }

    fn cycle_historical_relink(
        fixture: &mut HistoricalRelinkFixture,
        restored: &RestoredRelink,
    ) -> String {
        let cycled_postcondition = publish_review_and_relink(
            &mut fixture.authority,
            "return-review-2",
            fixture.schema.clone(),
            json!("second"),
        );
        let LiveEvolutionOutcome::PublicationApplied {
            receipt: cycled_receipt,
        } = &cycled_postcondition.receipt.outcome
        else {
            panic!("cycled historical publication returned the wrong outcome")
        };
        let cycled_decision = cycled_receipt.updates[0].decision_id.clone().unwrap();
        assert_eq!(
            cycled_decision,
            update_decision(
                "template-review",
                &restored.decision_id,
                &fixture.first_plan,
                &fixture.second_plan,
                RolloutMode::Active,
            )
            .unwrap()
            .decision_id
        );
        assert_ne!(cycled_decision, fixture.second_decision);
        assert_ne!(cycled_decision, restored.decision_id);
        assert!(
            cycled_postcondition
                .mutations
                .iter()
                .all(|mutation| !matches!(
                    mutation,
                    EvolutionMutation::PlanRecord(_)
                        | EvolutionMutation::LinkRecord(_)
                        | EvolutionMutation::EdgeRecord(_)
                ))
        );
        let cycled_publication_evidence = publication_evidence("return-review-2");
        assert_eq!(
            cycled_postcondition.artifacts,
            vec![cycled_publication_evidence.clone()]
        );
        let retained_evidence = [
            &fixture.advanced.receipt.command,
            &restored.postcondition.receipt.command,
            &cycled_postcondition.receipt.command,
        ]
        .map(|command| match &command.command {
            LiveEvolutionCommand::PublishAndRelink { publication, .. } => {
                publication.evidence.reference.clone()
            }
            _ => panic!("historical relink receipt retained the wrong command"),
        });
        assert_eq!(retained_evidence[0], fixture.first_edge.evidence);
        assert_eq!(retained_evidence[1], restored.edge.evidence);
        assert_eq!(retained_evidence[2], cycled_publication_evidence.reference);
        assert_ne!(retained_evidence[0], retained_evidence[1]);
        assert_ne!(retained_evidence[0], retained_evidence[2]);
        assert_ne!(retained_evidence[1], retained_evidence[2]);
        cycled_decision
    }

    fn assert_retained_historical_relink(
        fixture: &HistoricalRelinkFixture,
        restored: &RestoredRelink,
        cycled_decision: &str,
    ) {
        let view = fixture.authority.view("evolution-main");
        assert_eq!(
            edge_record(
                &view,
                "evolution-main",
                "template-review",
                &fixture.first_edge.edge.edge_id,
            )
            .unwrap()
            .unwrap(),
            &fixture.first_edge
        );
        assert_eq!(
            edge_record(
                &view,
                "evolution-main",
                "template-review",
                &restored.edge.edge.edge_id,
            )
            .unwrap()
            .unwrap(),
            &restored.edge
        );
        assert_eq!(
            rollout_evidence_current(
                &view,
                "evolution-main",
                "template-review",
                &fixture.second_decision,
            )
            .unwrap()
            .unwrap(),
            &fixture.second_evidence
        );
        let cycled_evidence =
            rollout_evidence_current(&view, "evolution-main", "template-review", cycled_decision)
                .unwrap()
                .unwrap();
        assert_ne!(
            cycled_evidence.decision_id,
            fixture.second_evidence.decision_id
        );
        assert_eq!(cycled_evidence.evidence_count, 0);
        let current = definition_current(&view, "evolution-main", "review-flow", "current")
            .unwrap()
            .unwrap();
        assert_eq!(current.max_sequence, 2);
    }

    #[test]
    fn historical_relink_reuses_plan_link_and_edge_without_overwriting_evidence() {
        let mut fixture = historical_relink_fixture();
        let restored = restore_historical_relink(&mut fixture);
        let cycled_decision = cycle_historical_relink(&mut fixture, &restored);
        assert_retained_historical_relink(&fixture, &restored, &cycled_decision);
    }
}
