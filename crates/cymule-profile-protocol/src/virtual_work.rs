//! Virtual-work profile persistence wire authority.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::OnceLock;

use crate::resource::{
    ResourceArchiveRelease, ResourceHandle, ResourceIntegrity, ResourcePinKind, ResourcePinReceipt,
    ResourcePublication, ResourceReleaseReceipt, ResourceRetentionSubject, ResourceShape,
};
use crate::{ProtocolError, ProtocolResult};
use cymule_core::{ArtifactRecord, ArtifactRef, ReplayAvailability};
use cymule_durable_protocol::{ClockObservation, ClockObservationRef, WaitActivationReceipt};
use serde::{Deserialize, Serialize};

/// Binding-pinned virtual work occurrence version.
pub const VIRTUAL_WORK_OCCURRENCE_VERSION: &str = "cymule.virtual-work-occurrence/3";
/// Provider-neutral virtual work control command version.
pub const VIRTUAL_WORK_CONTROL_VERSION: &str = "cymule.virtual-work-control/2";
/// Provider-neutral virtual region migration version.
pub const VIRTUAL_REGION_MIGRATION_VERSION: &str = "cymule.virtual-region-migration/3";
/// Provider-neutral virtual region migration control version.
pub const VIRTUAL_REGION_MIGRATION_CONTROL_VERSION: &str =
    "cymule.virtual-region-migration-control/3";
/// Immutable cold-archive manifest version.
pub const VIRTUAL_ARCHIVE_MANIFEST_VERSION: &str = "cymule.virtual-archive-manifest/2";
/// Verified virtual subtree compaction certificate version.
pub const VIRTUAL_COMPACTION_CERTIFICATE_VERSION: &str = "cymule.virtual-compaction-certificate/4";
/// Idempotent virtual compaction command version.
pub const VIRTUAL_COMPACTION_CONTROL_VERSION: &str = "cymule.virtual-compaction-control/1";
/// Idempotent partial rehydration command version.
pub const VIRTUAL_REHYDRATION_CONTROL_VERSION: &str = "cymule.virtual-rehydration-control/1";
/// Idempotent worker-slot claim command version.
pub const VIRTUAL_CLAIM_CONTROL_VERSION: &str = "cymule.virtual-claim-control/4";
/// Idempotent active-claim lease renewal command version.
pub const VIRTUAL_LEASE_RENEWAL_CONTROL_VERSION: &str = "cymule.virtual-lease-renewal-control/2";
/// Idempotent expired-claim recovery command version.
pub const VIRTUAL_RECOVERY_CONTROL_VERSION: &str = "cymule.virtual-recovery-control/2";
/// Idempotent future Run scheduling-weight update command version.
pub const VIRTUAL_RUN_WEIGHT_CONTROL_VERSION: &str = "cymule.virtual-run-weight-control/1";
/// Idempotent archive-certificate retirement control version.
pub const VIRTUAL_ARCHIVE_RETIREMENT_CONTROL_VERSION: &str =
    "cymule.virtual-archive-retirement-control/1";
/// Closed semantic virtual-persistence envelope version.
pub const VIRTUAL_PERSISTENCE_COMMAND_VERSION: &str = "cymule.virtual-persistence-command/2";
/// Keyed bounded current projection for one virtual scheduler.
pub const VIRTUAL_CURRENT_VERSION: &str = "cymule.virtual-current/3";
/// Receipt-independent semantic body of one keyed current projection.
pub const VIRTUAL_CURRENT_BODY_VERSION: &str = "cymule.virtual-current-body/2";
/// Exact receipt returned by one closed virtual persistence command.
pub const VIRTUAL_PERSISTENCE_RECEIPT_VERSION: &str = "cymule.virtual-persistence-receipt/3";
/// Ordered typed Virtual `StateRoot` mutation-set generation.
pub const VIRTUAL_MUTATION_SET_VERSION: &str = "cymule.virtual-mutation-set/2";
/// Closed scheduler initialization command version.
pub const VIRTUAL_INITIALIZATION_CONTROL_VERSION: &str = "cymule.virtual-initialization-control/2";
/// Closed materialized-page admission command version.
pub const VIRTUAL_MATERIALIZATION_CONTROL_VERSION: &str =
    "cymule.virtual-materialization-control/2";
/// Closed M1 activation-consumption command version.
pub const VIRTUAL_ACTIVATION_CONTROL_VERSION: &str = "cymule.virtual-activation-control/1";
/// Hard bound for exact Artifact bytes returned by one materialization page.
pub const MAX_MATERIALIZED_PAGE_ARTIFACT_BYTES: usize = 4 * 1024 * 1024;
/// Hard item bound for the ready plus active frontier embedded in one current leaf.
pub const MAX_VIRTUAL_CURRENT_FRONTIER_ITEMS: usize = 4_096;
/// Hard canonical byte bound for one virtual current leaf.
///
/// Durable's `StateRoot` leaf ceiling is larger; conformance asserts this strict
/// profile-local ceiling remains below it.
pub const MAX_VIRTUAL_CURRENT_BYTES: usize = 4 * 1024 * 1024;
/// Hard canonical byte bound for one normalized keyed Virtual leaf.
pub const MAX_VIRTUAL_KEYED_LEAF_BYTES: usize = 1024 * 1024;
/// Hard number of work identities one semantic command may add or move.
pub const MAX_VIRTUAL_MUTATION_ITEMS: usize = 1_024;
/// Hard aggregate number of normalized leaf changes in one semantic transition.
///
/// Migration is the widest transition: every source and target changes both
/// its audit region and active-order leaf, plus one migration receipt.
pub const MAX_VIRTUAL_MUTATION_SET_ITEMS: usize = MAX_VIRTUAL_MUTATION_ITEMS * 4 + 1;
/// Hard aggregate count of exact parent membership/non-membership reads.
///
/// The widest migration proves both `Regions` and `ActiveRegions` membership for
/// every source, the same two-family absence for every target, and exact
/// migration-receipt absence. It therefore has the same item width as the
/// resulting mutation set.
pub const MAX_VIRTUAL_REDUCTION_SOURCE_ITEMS: usize = MAX_VIRTUAL_MUTATION_SET_ITEMS;
/// Hard canonical byte bound for one complete typed mutation set.
pub const MAX_VIRTUAL_MUTATION_BYTES: usize = 4 * 1024 * 1024;
/// Hard canonical byte bound for one semantic persistence command.
///
/// The bound admits the 4 MiB exact Artifact product plus typed control data.
pub const MAX_VIRTUAL_PERSISTENCE_COMMAND_BYTES: usize = 5 * 1024 * 1024;
/// Hard canonical byte bound for one persisted Virtual receipt leaf.
///
/// Durable permits a 12 MiB `StateRoot` leaf. Virtual reserves 2 MiB for the
/// `StateRoot` value envelope and future non-payload fields, so every accepted
/// receipt is physically representable by the real Durable leaf contract.
pub const MAX_VIRTUAL_PERSISTENCE_RECEIPT_BYTES: usize = 10 * 1024 * 1024;
/// Hard canonical byte bound for one non-persisted Virtual control response.
///
/// This matches Durable's physical `StateRoot` leaf ceiling. The embedded
/// receipt remains subject to its stricter 10 MiB limit, leaving bounded room
/// for revision pins and the response envelope.
pub const MAX_VIRTUAL_CONTROL_ENVELOPE_BYTES: usize = 12 * 1024 * 1024;
/// Hard aggregate canonical byte bound for exact reducer source leaves.
///
/// This equals one 4 MiB current plus one 4 MiB mutation parent set; the
/// reducer therefore cannot turn many individually legal leaves into an
/// unbounded in-memory authority bundle.
pub const MAX_VIRTUAL_REDUCTION_SOURCE_BYTES: usize = 8 * 1024 * 1024;
/// Hard canonical byte bound for one immutable Virtual archive object.
///
/// This matches the framework's bounded archive provider write contract, so a
/// reducer never accepts an object that cannot be written and read back in one
/// exact provider operation.
pub const MAX_VIRTUAL_ARCHIVE_BYTES: usize = 8 * 1024 * 1024;
/// Hard number of parked identities retained by one reason-index page.
pub const MAX_VIRTUAL_PARKED_INDEX_PAGE_ITEMS: usize = 256;
/// A materialization successor proof may read once after the retained cursor
/// and, only when that suffix is empty, once from the authenticated map head.
pub const MAX_VIRTUAL_ACTIVE_REGION_SELECTION_PAGES: u8 = 2;
/// Hard logical byte bound for one non-Serde authenticated region page witness:
/// three content IDs plus exact count and terminal flag.
pub const MAX_VIRTUAL_ACTIVE_REGION_PAGE_BYTES: usize = 3 * ("sha256:".len() + 64) + 8 + 1;
/// Hard logical byte bound for the closed one-page-or-wrap selection witness:
/// three content IDs, the worst-case UTF-8 encoding of 512 identity scalars,
/// exact count, and page count.
pub const MAX_VIRTUAL_ACTIVE_REGION_SELECTION_BYTES: usize =
    3 * ("sha256:".len() + 64) + 512 * 4 + 8 + 1;
/// Content domain deriving the only M1 journal owned by one scheduler.
pub const VIRTUAL_SCHEDULER_JOURNAL_ID_DOMAIN: &str = "cymule.virtual-scheduler-journal-id/1";
/// Stable media type for the canonical Virtual cold-history object.
pub const VIRTUAL_ARCHIVE_MANIFEST_KIND: &str =
    "application/vnd.cymule.virtual-archive-manifest+json";

const COMMAND_INDEX_LEAF_DOMAIN: &str = "cymule.virtual-archive-command-index-leaf/1";
const COMMAND_INDEX_NODE_DOMAIN: &str = "cymule.virtual-archive-command-index-node/1";
const COMMAND_INDEX_EMPTY_LEAF_DOMAIN: &str = "cymule.virtual-archive-command-index-empty-leaf/1";
const COMMAND_INDEX_KEY_DOMAIN: &[u8] = b"cymule.virtual-archive-command-index-key/1";
const COMMAND_INDEX_PROOF_VERSION: &str = "cymule.virtual-archive-command-index-proof/1";
const COMMAND_INDEX_DEPTH: usize = 256;
const WORK_INDEX_LEAF_DOMAIN: &str = "cymule.virtual-archive-work-leaf/1";
const WORK_INDEX_NODE_DOMAIN: &str = "cymule.virtual-archive-work-node/1";
const WORK_INDEX_EMPTY_LEAF_DOMAIN: &str = "cymule.virtual-archive-work-empty-leaf/1";
const WORK_INDEX_KEY_DOMAIN: &[u8] = b"cymule.virtual-archive-work-key/1";
const WORK_INDEX_PROOF_VERSION: &str = "cymule.virtual-archive-work-proof/1";
const WORK_INDEX_DEPTH: usize = 256;
const VIRTUAL_STATE_STORAGE_KEY_DOMAIN: &str = "cymule.virtual-state-storage-key/1";
/// Content domain sealing one physical normalized-family map descriptor.
pub const VIRTUAL_STATE_ROOT_ID_DOMAIN: &str = "cymule.virtual-state-root/1";
/// Content domain deriving one scheduler's scalar-current `StateRoot` key.
pub const VIRTUAL_CURRENT_STORAGE_KEY_DOMAIN: &str = "cymule.virtual-current-storage-key/1";
/// Content domain deriving one scheduler-and-command all-ever receipt key.
pub const VIRTUAL_RECEIPT_STORAGE_KEY_DOMAIN: &str = "cymule.virtual-receipt-storage-key/1";
const OCCURRENCE_LEAF_DOMAIN: &str = "cymule.virtual-archive-occurrence-leaf/1";
const OCCURRENCE_NODE_DOMAIN: &str = "cymule.virtual-archive-occurrence-node/1";
const COMMAND_LEAF_DOMAIN: &str = "cymule.virtual-archive-command-leaf/1";
const COMMAND_NODE_DOMAIN: &str = "cymule.virtual-archive-command-node/1";
/// Normalized region-leaf generation.
pub const VIRTUAL_REGION_CURRENT_VERSION: &str = "cymule.virtual-region-current/1";
/// Normalized materializable-region order leaf generation.
pub const VIRTUAL_ACTIVE_REGION_CURRENT_VERSION: &str = "cymule.virtual-active-region-current/1";
/// Normalized parked-work-leaf generation.
pub const VIRTUAL_PARKED_CURRENT_VERSION: &str = "cymule.virtual-parked-current/1";
/// Normalized parked-reason index-page generation.
pub const VIRTUAL_PARKED_INDEX_PAGE_VERSION: &str = "cymule.virtual-parked-index-page/1";
/// Normalized hot-work-leaf generation.
pub const VIRTUAL_WORK_CURRENT_VERSION: &str = "cymule.virtual-work-current/1";
/// Normalized occurrence-leaf generation.
pub const VIRTUAL_OCCURRENCE_CURRENT_VERSION: &str = "cymule.virtual-occurrence-current/1";
/// Normalized Run-fairness-leaf generation.
pub const VIRTUAL_RUN_CURRENT_VERSION: &str = "cymule.virtual-run-current/1";
/// Normalized migration-leaf generation.
pub const VIRTUAL_MIGRATION_CURRENT_VERSION: &str = "cymule.virtual-migration-current/1";
/// Normalized certificate-leaf generation.
pub const VIRTUAL_CERTIFICATE_CURRENT_VERSION: &str = "cymule.virtual-certificate-current/1";

/// Opaque provider-neutral durable cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCursor {
    /// Source-owned version/domain.
    pub version: String,
    /// Opaque logical position.
    pub position: String,
    /// Whether the source is exhausted.
    pub exhausted: bool,
}

/// Exact provider-neutral identity of one `RegionSource` adapter generation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionSourceBinding {
    /// Semantic source operation implemented by the adapter.
    pub operation: String,
    /// Immutable adapter binding identity.
    pub binding: String,
    /// Immutable implementation revision within that binding.
    pub revision: String,
}

/// Exact immutable provider generation owning Virtual cold objects and both
/// cumulative authenticated indexes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveBinding {
    /// Immutable archive adapter binding identity.
    pub binding: String,
    /// Immutable implementation revision within that binding.
    pub revision: String,
}

impl VirtualArchiveBinding {
    /// Construct one exact immutable archive/index provider generation.
    ///
    /// # Errors
    ///
    /// Returns an error when either selector is empty, contains a control
    /// character, or exceeds the provider-selector bound.
    pub fn new(binding: impl Into<String>, revision: impl Into<String>) -> ProtocolResult<Self> {
        let value = Self {
            binding: binding.into(),
            revision: revision.into(),
        };
        value.verify()?;
        Ok(value)
    }

    /// Verify this immutable provider selector.
    ///
    /// # Errors
    ///
    /// Returns an error when either selector is empty, contains a control
    /// character, or exceeds the provider-selector bound.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_archive_binding(self)
    }
}

/// Immutable execution-selection policy owned by one Virtual Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum VirtualRunExecution {
    /// Always execute one exact already-admitted Plan.
    Direct {
        /// Exact semantic Plan identity.
        plan_id: String,
    },
    /// Select the current compatible Plan from one M4 template when a work
    /// occurrence is claimed.
    Evolution {
        /// Exact M4 authority partition.
        evolution_id: String,
        /// Registered template within that partition.
        template_id: String,
    },
}

/// Complete immutable definition of one fairness Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRunDefinition {
    /// Stable Run/fairness namespace referenced by regions and work.
    pub run_id: String,
    /// Exact Plan-selection authority for every future occurrence in the Run.
    pub execution: VirtualRunExecution,
}

impl VirtualRunExecution {
    /// Verify one immutable Direct or Evolution execution selector.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        match self {
            Self::Direct { plan_id } => validate_content_id("Virtual Run Plan", plan_id),
            Self::Evolution {
                evolution_id,
                template_id,
            } => {
                validate_identity("Virtual Run Evolution authority", evolution_id)?;
                validate_identity("Virtual Run Evolution template", template_id)
            }
        }
    }
}

impl VirtualRunDefinition {
    /// Verify one complete immutable Run definition.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Virtual Run", &self.run_id)?;
        self.execution.verify()
    }
}

/// Exact source state observed by a verified region migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionSourceCheckpoint {
    /// Exact adapter generation that owned the cursor.
    pub source: RegionSourceBinding,
    /// Exact opaque cursor at migration admission.
    pub cursor: VirtualCursor,
}

/// A logical region whose full work set is not materialized eagerly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRegion {
    /// Stable region identity.
    pub region_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Exact source operation, adapter binding, and implementation revision.
    pub source: RegionSourceBinding,
    /// Immutable source configuration or metadata Artifact interpreted by the
    /// pinned source adapter generation.
    pub source_artifact: ArtifactRef,
    /// Current durable cursor.
    pub cursor: VirtualCursor,
    /// Optional logical cardinality estimate for display only.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub estimated_total: Option<u64>,
}

/// One materialized work item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItem {
    /// Stable source-derived work identity.
    pub work_id: String,
    /// Owning virtual region.
    pub region_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Immutable typed payload.
    pub payload: ArtifactRef,
    /// Required worker capability, when any.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub capability: Option<String>,
    /// Relative scheduling priority. Higher values run first within a Run.
    pub priority: i32,
    /// Provider-neutral budget weight.
    pub cost: u64,
}

/// One bounded page returned by a `RegionSource`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedPage {
    /// Newly visible work.
    pub items: Vec<WorkItem>,
    /// Bounded immutable payload records supplied by this page.
    ///
    /// Every record must be referenced by at least one item in this page. The
    /// durable controller publishes these bytes in the same CAS as the cursor
    /// and frontier transition.
    pub artifacts: Vec<ArtifactRecord>,
    /// Cursor after this exact page.
    pub next_cursor: VirtualCursor,
}

/// Why work is not currently schedulable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParkReason {
    /// Waiting for a durable condition.
    Wait {
        /// Durable wait correlation key.
        key: String,
    },
    /// Waiting for another work identity.
    Dependency {
        /// Required predecessor work identity.
        work_id: String,
    },
    /// Waiting for budget availability.
    Budget {
        /// Budget account or dimension.
        account: String,
    },
    /// Waiting for a compatible worker.
    Capability {
        /// Required worker capability.
        capability: String,
    },
    /// Explicit scheduler backpressure domain.
    Backpressure {
        /// Scheduler-defined pressure domain.
        domain: String,
    },
}

/// Parked work plus its indexed reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParkedWork {
    /// Work item.
    pub item: WorkItem,
    /// Indexed wake reason.
    pub reason: ParkReason,
}

/// Fenced active claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualClaimLease {
    /// Stable worker capacity-slot resource.
    pub resource: String,
    /// Current lease holder.
    pub owner: String,
    /// Monotone fencing epoch for the capacity slot.
    pub epoch: u64,
    /// Logical expiry supplied by a Clock substrate.
    pub expires_at: u64,
    /// Latest exact Clock receipt reference on this lease's immutable timeline.
    pub clock: ClockObservationRef,
}

/// Fenced active claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimedWork {
    /// Work item.
    pub item: WorkItem,
    /// Claim owner.
    pub owner: String,
    /// Monotone per-work fencing epoch.
    pub epoch: u64,
    /// Stable identity of this exact work attempt occurrence.
    pub occurrence_id: String,
    /// Exact semantic Plan selected independently from implementation binding.
    pub plan_id: String,
    /// Exact admitted `cymule.execution-binding/2` Artifact.
    pub execution_binding: ArtifactRef,
    /// Current fenced worker capacity-slot lease.
    pub lease: VirtualClaimLease,
}

/// Lifecycle of one binding-pinned work attempt occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkOccurrenceState {
    /// The fenced claim may still produce one disposition.
    Running,
    /// The work produced a terminal result.
    Succeeded,
    /// The attempt failed and the same logical work was scheduled again.
    RetryScheduled,
    /// The attempt yielded to an indexed parked condition.
    Parked,
    /// The logical work ended with a terminal failure.
    Failed,
    /// The active attempt was cancelled and fenced from later completion.
    Cancelled,
}

/// One immutable-identity attempt and its current durable disposition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkOccurrence {
    /// Occurrence schema and semantic version.
    pub occurrence_version: String,
    /// Stable identity derived from work ID and claim epoch.
    pub occurrence_id: String,
    /// Logical work identity shared by retries.
    pub work_id: String,
    /// Owning virtual region.
    pub region_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Claim owner for this exact attempt.
    pub owner: String,
    /// Monotone per-work claim epoch.
    pub epoch: u64,
    /// Current or terminal capacity-slot lease fence for this attempt.
    pub lease_epoch: u64,
    /// Latest exact Clock receipt reference admitted for this occurrence.
    pub lease_clock: ClockObservationRef,
    /// Exact semantic Plan pinned for this attempt.
    pub plan_id: String,
    /// Exact admitted `cymule.execution-binding/2` Artifact.
    pub execution_binding: ArtifactRef,
    /// Current occurrence lifecycle.
    pub state: WorkOccurrenceState,
    /// Terminal output Artifact for success.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub result: Option<ArtifactRef>,
    /// Failure or cancellation evidence Artifact.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub error: Option<ArtifactRef>,
    /// Indexed condition used by retry or park, when present.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub next_reason: Option<ParkReason>,
}

/// Fenced disposition proposed for one active work occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "resolution", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkResolution {
    /// Publish one terminal result.
    Succeeded {
        /// Immutable typed output.
        result: ArtifactRef,
    },
    /// Retain failure evidence and schedule another claim.
    Retry {
        /// Immutable failure evidence.
        error: ArtifactRef,
        /// Optional indexed condition; `None` requeues immediately.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        next_reason: Option<ParkReason>,
    },
    /// Yield the work without classifying the attempt as failed.
    Parked {
        /// Exact indexed condition required to wake the work.
        reason: ParkReason,
    },
    /// Publish one terminal failure.
    Failed {
        /// Immutable terminal failure evidence.
        error: ArtifactRef,
    },
    /// Cancel the active occurrence.
    Cancelled {
        /// Immutable cancellation reason or evidence.
        reason: ArtifactRef,
    },
}

/// Idempotent control command resolving one fenced work occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkResolutionCommand {
    /// Control command schema and semantic version.
    pub control_version: String,
    /// Stable caller-generated idempotency identity.
    pub command_id: String,
    /// Logical work identity.
    pub work_id: String,
    /// Expected current claim owner.
    pub owner: String,
    /// Expected current claim epoch.
    pub epoch: u64,
    /// Expected current capacity-slot lease epoch.
    pub expected_lease_epoch: u64,
    /// Opaque current-head observation issued by the selected durable Clock.
    pub clock: ClockObservationRef,
    /// Proposed terminal, retry, park, or cancellation disposition.
    pub resolution: WorkResolution,
}

/// Durable exact result of one normal fenced work-resolution command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkResolutionReceipt {
    /// Exact admitted idempotent command.
    pub command: WorkResolutionCommand,
    /// Exact terminal or yielded occurrence returned by the command.
    pub occurrence: WorkOccurrence,
}

/// Idempotent request to claim at most one work item through a capacity slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualClaimCommand {
    /// Control schema and semantic version.
    pub control_version: String,
    /// Stable caller-generated command identity.
    pub command_id: String,
    /// Stable worker identity.
    pub owner: String,
    /// Stable capacity-slot resource owned by the worker substrate.
    pub slot_id: String,
    /// Exact pre-existing `cymule.execution-binding/2` Artifact selected by
    /// the worker. Durable resolves and verifies its bytes from the same
    /// pinned Machine authority; claim never admits new binding bytes.
    pub execution_binding: ArtifactRef,
    /// Worker capabilities used for deterministic selection.
    pub capabilities: BTreeSet<String>,
    /// Opaque current-head observation issued for this exact capacity slot.
    pub clock: ClockObservationRef,
    /// Positive logical lease duration.
    pub lease_ttl: u64,
}

/// Exact typed link to the standard Evolution selection committed with a
/// Virtual claim.
///
/// This is not a second Evolution receipt. Durable stores and resolves the
/// standard Evolution receipt and normalized occurrence/selection leaves; the
/// link only binds the Virtual outcome to that exact result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualEvolutionSelectionLink {
    /// Exact resulting scalar Evolution current.
    pub evolution_current: crate::evolution::EvolutionCurrent,
    /// Standard Evolution persistence receipt committed by the same CAS.
    pub receipt_id: String,
    /// Complete retained selection contract and runtime binding.
    pub pin: crate::evolution::OccurrencePin,
}

/// Durable result of one worker-slot claim command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualClaimReceipt {
    /// Exact admitted command.
    pub command: VirtualClaimCommand,
    /// Complete Clock receipt resolved before the claim CAS.
    pub clock_observation: ClockObservation,
    /// Claimed work, or a durable observation that no work was eligible.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub claim: Option<ClaimedWork>,
    /// Exact configured execution selector of the claimed Run, or null for an
    /// empty claim.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub run_execution: Option<VirtualRunExecution>,
    /// Exact standard M4 selection committed with this claim, or null for a
    /// standalone or empty M3 claim.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub evolution_selection: Option<VirtualEvolutionSelectionLink>,
}

/// Idempotent request to extend one active claim under a later slot epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualLeaseRenewalCommand {
    /// Control schema and semantic version.
    pub control_version: String,
    /// Stable caller-generated command identity.
    pub command_id: String,
    /// Logical work identity.
    pub work_id: String,
    /// Expected active owner.
    pub owner: String,
    /// Expected work occurrence epoch.
    pub epoch: u64,
    /// Expected current capacity-slot lease epoch.
    pub expected_lease_epoch: u64,
    /// Opaque current-head observation issued for the active capacity slot.
    pub clock: ClockObservationRef,
    /// Positive logical lease duration.
    pub lease_ttl: u64,
}

/// Durable result of one active-claim lease renewal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualLeaseRenewalReceipt {
    /// Exact admitted command.
    pub command: VirtualLeaseRenewalCommand,
    /// Complete Clock receipt resolved before the renewal CAS.
    pub clock_observation: ClockObservation,
    /// New fenced lease retained by the active claim.
    pub lease: VirtualClaimLease,
}

/// Idempotent recovery decision for an expired active claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRecoveryCommand {
    /// Control schema and semantic version.
    pub control_version: String,
    /// Stable caller-generated command identity.
    pub command_id: String,
    /// Logical work identity.
    pub work_id: String,
    /// Expected failed/stale worker owner.
    pub expected_owner: String,
    /// Expected work occurrence epoch.
    pub expected_epoch: u64,
    /// Expected expired capacity-slot lease epoch.
    pub expected_lease_epoch: u64,
    /// Opaque current-head observation proving expiry for the capacity slot.
    pub clock: ClockObservationRef,
    /// Explicit retry, terminal failure, or cancellation decision.
    pub resolution: WorkResolution,
}

/// Durable result of one expired-claim recovery decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRecoveryReceipt {
    /// Exact admitted command.
    pub command: VirtualRecoveryCommand,
    /// Complete Clock receipt resolved before the recovery CAS.
    pub clock_observation: ClockObservation,
    /// Original occurrence after its recovery disposition.
    pub occurrence: WorkOccurrence,
}

/// Idempotent update to one Run's future weighted scheduling share.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRunWeightCommand {
    /// Control schema and semantic version.
    pub control_version: String,
    /// Stable caller-generated command identity.
    pub command_id: String,
    /// Registered Run whose future share changes.
    pub run_id: String,
    /// Positive integer future scheduling weight.
    pub weight: u32,
}

/// Durable receipt for one Run scheduling-weight update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRunWeightReceipt {
    /// Exact admitted command.
    pub command: VirtualRunWeightCommand,
    /// Previous positive Run weight.
    pub previous_weight: u32,
    /// New positive Run weight.
    pub current_weight: u32,
}

/// Closed virtual-region topology migration kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegionMigrationKind {
    /// Retire one region and replace it with two or more regions.
    Split,
    /// Retire two or more regions and replace them with one region.
    Merge,
}

/// Caller request given to a replaceable region migration adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionMigrationRequest {
    /// Stable migration identity.
    pub migration_id: String,
    /// Desired split or merge transition.
    pub kind: RegionMigrationKind,
    /// Exact active source region IDs.
    pub source_region_ids: BTreeSet<String>,
    /// Desired number of target regions.
    pub target_count: usize,
    /// Immutable adapter binding selected for this migration occurrence.
    pub migration_binding: String,
    /// Immutable implementation revision within the selected binding.
    pub migration_revision: String,
}

/// Adapter-produced opaque cursor migration with coverage evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionMigrationPlan {
    /// Migration schema and semantic version.
    pub migration_version: String,
    /// Stable migration identity.
    pub migration_id: String,
    /// Split or merge topology transition.
    pub kind: RegionMigrationKind,
    /// Exact source adapter generations and cursors observed by the migrator.
    pub expected_sources: BTreeMap<String, RegionSourceCheckpoint>,
    /// Replacement regions covering the remaining source domain.
    pub targets: Vec<VirtualRegion>,
    /// Immutable adapter binding that produced and can verify this plan.
    pub migration_binding: String,
    /// Immutable implementation revision that produced this plan.
    pub migration_revision: String,
    /// Immutable proof or attestation of non-overlapping complete coverage.
    pub coverage_evidence: ArtifactRef,
}

/// Durable receipt retaining one applied region migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionMigrationReceipt {
    /// Exact applied plan.
    pub plan: RegionMigrationPlan,
    /// Retired source region IDs.
    pub retired_regions: BTreeSet<String>,
    /// Newly active target region IDs.
    pub active_targets: BTreeSet<String>,
}

/// Idempotent control command applying an adapter-produced migration plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionMigrationCommand {
    /// Control command schema and semantic version.
    pub control_version: String,
    /// Stable caller-generated idempotency identity.
    pub command_id: String,
    /// Exact adapter-produced plan.
    pub plan: RegionMigrationPlan,
}

/// Terminal logical-work index retained when occurrence payloads move cold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedWorkIndex {
    /// Stable logical work identity.
    pub work_id: String,
    /// Owning virtual region.
    pub region_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Greatest archived occurrence identity for this logical work.
    pub occurrence_id: String,
    /// Greatest fenced occurrence epoch represented by the manifest.
    pub max_epoch: u64,
    /// Terminal state of the greatest epoch.
    pub terminal_state: WorkOccurrenceState,
}

/// Side of one sibling in a Virtual archive Merkle path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VirtualArchiveMerkleSide {
    /// The sibling hashes before the current node.
    Left,
    /// The sibling hashes after the current node.
    Right,
}

/// One sibling step in a Virtual archive Merkle path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveMerkleStep {
    /// Position of the sibling relative to the current node.
    pub side: VirtualArchiveMerkleSide,
    /// Exact lowercase SHA-256 identity of the sibling node.
    pub digest: String,
}

/// Bounded proof locating one exact occurrence inside an immutable archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveOccurrenceProof {
    /// Exact occurrence identity.
    pub occurrence_id: String,
    /// Zero-based position in the canonical occurrence map.
    pub index: u64,
    /// Byte offset of the canonical occurrence value in the archive Resource.
    pub offset: u64,
    /// Exact canonical occurrence byte length.
    pub length: u64,
    /// Digest of the canonical occurrence value.
    pub digest: String,
    /// Sibling path from the occurrence leaf to the certificate root.
    pub path: Vec<VirtualArchiveMerkleStep>,
}

/// Bounded proof locating one exact historical command receipt in one
/// immutable archive object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveCommandProof {
    /// Durable application journal that originally owned the command.
    pub journal_id: String,
    /// Stable historical command identity.
    pub command_id: String,
    /// Zero-based position in the canonical command-receipt map.
    pub index: u64,
    /// Byte offset of the canonical receipt value in the archive Resource.
    pub offset: u64,
    /// Exact canonical receipt byte length.
    pub length: u64,
    /// Digest of the canonical receipt value.
    pub digest: String,
    /// Sibling path from the command leaf to the certificate root.
    pub path: Vec<VirtualArchiveMerkleStep>,
}

/// Exact typed historical receipt returned by a binding-pinned archive
/// provider together with its immutable range proof.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchivedCommand {
    /// Complete self-verifying Virtual persistence receipt.
    pub receipt: VirtualPersistenceReceipt,
    /// Certificate-bound exact range proof for the receipt bytes.
    pub proof: VirtualArchiveCommandProof,
}

/// Membership or non-membership proof in the cumulative archived-work map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveWorkProof {
    /// Proof schema and semantic version.
    pub proof_version: String,
    /// Exact logical work identity whose hashed path is proven.
    pub work_id: String,
    /// Archived identity and terminal fence for membership; absent otherwise.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub value: Option<ArchivedWorkIndex>,
    /// Depth of the canonical empty subtree proving non-membership.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub empty_depth: Option<u16>,
    /// Sibling hashes above the leaf or empty subtree, ordered toward the root.
    pub siblings: Vec<String>,
}

/// One immutable content-addressed node in the cumulative archived-work map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum VirtualArchiveWorkIndexNode {
    /// Internal fixed-depth sparse-Merkle node.
    Branch {
        /// Content-addressed node identity.
        node_id: String,
        /// Zero-based tree depth.
        depth: u16,
        /// Left child identity.
        left: String,
        /// Right child identity.
        right: String,
    },
    /// Occupied work-identity leaf.
    Member {
        /// Content-addressed leaf identity.
        node_id: String,
        /// Hashed 256-bit work path.
        key_hash: String,
        /// Exact terminal identity and greatest archived claim fence.
        value: ArchivedWorkIndex,
    },
}

/// One verified cumulative archived-work insertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveWorkIndexUpdate {
    /// Root against which non-membership was proven.
    pub parent_root_digest: String,
    /// Canonical non-membership proof for the inserted work identity.
    pub nonmembership: VirtualArchiveWorkProof,
    /// Exact terminal identity and greatest claim fence inserted.
    pub value: ArchivedWorkIndex,
    /// Resulting cumulative map root.
    pub result_root_digest: String,
}

/// Exact cold locator for one command receipt removed from the hot journal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedCommandIndex {
    /// Durable application journal that originally owned the command.
    pub journal_id: String,
    /// Stable historical command identity.
    pub command_id: String,
    /// Exact immutable certificate authenticating the command range proof.
    pub certificate_id: String,
    /// Exact immutable archive Resource containing the command receipt bytes.
    pub archive_resource_id: String,
}

/// Membership or non-membership proof in the cumulative archived-command map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveCommandIndexProof {
    /// Proof schema and semantic version.
    pub proof_version: String,
    /// Durable application journal that owned the command.
    pub journal_id: String,
    /// Exact historical command identity whose hashed path is proven.
    pub command_id: String,
    /// Exact certificate and archive identity for membership; absent otherwise.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub value: Option<ArchivedCommandIndex>,
    /// Depth of the canonical empty subtree proving non-membership.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub empty_depth: Option<u16>,
    /// Sibling hashes above the leaf or empty subtree, ordered toward the root.
    pub siblings: Vec<String>,
}

/// One immutable content-addressed node in the cumulative archived-command map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum VirtualArchiveCommandIndexNode {
    /// Internal fixed-depth sparse-Merkle node.
    Branch {
        /// Content-addressed node identity.
        node_id: String,
        /// Zero-based tree depth.
        depth: u16,
        /// Left child identity.
        left: String,
        /// Right child identity.
        right: String,
    },
    /// Occupied journal-and-command leaf.
    Member {
        /// Content-addressed leaf identity.
        node_id: String,
        /// Hashed 256-bit journal-and-command path.
        key_hash: String,
        /// Exact immutable certificate and archive locator.
        value: ArchivedCommandIndex,
    },
}

/// One verified cumulative archived-command insertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveCommandIndexUpdate {
    /// Root against which non-membership was proven.
    pub parent_root_digest: String,
    /// Canonical non-membership proof for the inserted command identity.
    pub nonmembership: VirtualArchiveCommandIndexProof,
    /// Exact immutable certificate and archive locator inserted.
    pub value: ArchivedCommandIndex,
    /// Resulting cumulative map root.
    pub result_root_digest: String,
}

/// Immutable cold archive payload owned by the Virtual profile.
///
/// Command history is retained as complete typed Virtual receipts. Raw M1
/// journal records and arbitrary schema/payload bytes are intentionally not
/// representable in this archive contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveManifest {
    /// Archive payload generation.
    pub manifest_version: String,
    /// Region whose completed history is represented.
    pub region_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Derived M1 application journal for archived typed receipts, when any.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub journal_id: Option<String>,
    /// Exact causally closed checkpoints covered by the archive.
    pub source_causal_cut: BTreeSet<String>,
    /// Exact terminal occurrence records keyed by occurrence identity.
    pub occurrences: BTreeMap<String, WorkOccurrence>,
    /// Greatest terminal fence for each archived work identity.
    pub work_index: BTreeMap<String, ArchivedWorkIndex>,
    /// Cumulative archived-work root before these insertions.
    pub parent_work_index_root_digest: String,
    /// Exact sequential absence insertions for every work identity.
    pub work_index_updates: Vec<VirtualArchiveWorkIndexUpdate>,
    /// Cumulative archived-work root after these insertions.
    pub result_work_index_root_digest: String,
    /// Exact typed command receipts keyed by semantic command identity.
    pub command_receipts: BTreeMap<String, VirtualPersistenceReceipt>,
}

/// Canonical immutable archive bytes and their exact bounded range proofs.
///
/// This is a pure provider-construction product, not serializable persistence
/// authority. Archive adapters persist these bytes and descriptor-scoped proof
/// catalogs, while Durable derives certificate roots from the same manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualArchiveLayout {
    /// Complete canonical manifest bytes.
    pub bytes: Vec<u8>,
    /// Merkle root authenticating every occurrence range.
    pub occurrence_root_digest: String,
    /// Exact occurrence proofs keyed by occurrence identity.
    pub occurrence_proofs: BTreeMap<String, VirtualArchiveOccurrenceProof>,
    /// Merkle root authenticating every typed command receipt, or null when none.
    pub command_root_digest: Option<String>,
    /// Exact typed command-receipt proofs keyed by command identity.
    pub command_proofs: BTreeMap<String, VirtualArchiveCommandProof>,
}

impl VirtualArchiveWorkIndexNode {
    /// Verify and return this immutable archived-work node's content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn identity(&self) -> ProtocolResult<&str> {
        match self {
            Self::Branch {
                node_id,
                depth,
                left,
                right,
            } => {
                let depth = usize::from(*depth);
                if depth >= WORK_INDEX_DEPTH
                    || !valid_content_id(left)
                    || !valid_content_id(right)
                    || work_index_node(depth, left, right)? != *node_id
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual archived-work index branch is malformed".to_owned(),
                    ));
                }
                Ok(node_id)
            }
            Self::Member {
                node_id,
                key_hash,
                value,
            } => {
                verify_archived_work_index(value)?;
                let key = work_index_key(&value.work_id)?;
                if key_hash != &sparse_index_key_id(&key)
                    || work_index_member_leaf(&key, value)? != *node_id
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual archived-work index member is malformed".to_owned(),
                    ));
                }
                Ok(node_id)
            }
        }
    }
}

impl VirtualArchiveWorkProof {
    /// Verify this membership or non-membership proof against one exact root.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self, expected_root: &str) -> ProtocolResult<()> {
        let depth = self.validate_shape()?;
        if !valid_content_id(expected_root) || self.root(depth)? != expected_root {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual archived-work proof does not reach its expected root".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> ProtocolResult<usize> {
        if self.proof_version != WORK_INDEX_PROOF_VERSION
            || validate_identity("Virtual archived work", &self.work_id).is_err()
            || self.siblings.iter().any(|value| !valid_content_id(value))
        {
            return Err(ProtocolError::Validation(
                "Virtual archived-work proof version, identity, or sibling is malformed".to_owned(),
            ));
        }
        let depth = match (&self.value, self.empty_depth) {
            (Some(value), None) if self.siblings.len() == WORK_INDEX_DEPTH => {
                verify_archived_work_index(value)?;
                if value.work_id != self.work_id {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual archived-work membership changed logical identity".to_owned(),
                    ));
                }
                WORK_INDEX_DEPTH
            }
            (None, Some(depth))
                if usize::from(depth) <= WORK_INDEX_DEPTH
                    && self.siblings.len() == usize::from(depth) =>
            {
                usize::from(depth)
            }
            _ => {
                return Err(ProtocolError::Validation(
                    "Virtual archived-work proof has an unsupported membership shape".to_owned(),
                ));
            }
        };
        if self.value.is_none()
            && depth > 0
            && self.siblings.first() == work_index_empty_hashes().get(depth)
        {
            return Err(ProtocolError::Validation(
                "Virtual archived-work non-membership proof is not maximally compressed".to_owned(),
            ));
        }
        Ok(depth)
    }

    fn root(&self, depth: usize) -> ProtocolResult<String> {
        self.root_with_value(depth, self.value.as_ref())
    }

    fn root_with_value(
        &self,
        proof_depth: usize,
        value: Option<&ArchivedWorkIndex>,
    ) -> ProtocolResult<String> {
        let key = work_index_key(&self.work_id)?;
        let empty = work_index_empty_hashes();
        let mut current = match (&self.value, value) {
            (Some(expected), Some(value)) if expected == value => {
                work_index_member_leaf(&key, value)?
            }
            (Some(_), _) => {
                return Err(ProtocolError::IllegalTransition(
                    "Virtual archived-work membership value cannot be replaced".to_owned(),
                ));
            }
            (None, Some(value)) => {
                verify_archived_work_index(value)?;
                if value.work_id != self.work_id {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual archived-work insertion changed logical identity".to_owned(),
                    ));
                }
                let mut current = work_index_member_leaf(&key, value)?;
                for level in 0..WORK_INDEX_DEPTH - proof_depth {
                    let depth = WORK_INDEX_DEPTH - level - 1;
                    let sibling = &empty[WORK_INDEX_DEPTH - level];
                    current = if sparse_index_bit(&key, depth) {
                        work_index_node(depth, sibling, &current)?
                    } else {
                        work_index_node(depth, &current, sibling)?
                    };
                }
                current
            }
            (None, None) => empty[proof_depth].clone(),
        };
        for (level, sibling) in self.siblings.iter().enumerate() {
            let depth = proof_depth - level - 1;
            current = if sparse_index_bit(&key, depth) {
                work_index_node(depth, sibling, &current)?
            } else {
                work_index_node(depth, &current, sibling)?
            };
        }
        Ok(current)
    }
}

/// Return the canonical empty cumulative archived-work root.
pub fn virtual_work_index_empty_root() -> String {
    work_index_empty_hashes()[0].clone()
}

/// Resolve one fixed-depth archived-work proof from immutable exact nodes.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn resolve_virtual_work_index_proof(
    root: &str,
    work_id: &str,
    mut load: impl FnMut(&str) -> ProtocolResult<Option<VirtualArchiveWorkIndexNode>>,
) -> ProtocolResult<VirtualArchiveWorkProof> {
    if !valid_content_id(root) || validate_identity("Virtual archived work", work_id).is_err() {
        return Err(ProtocolError::Validation(
            "Virtual archived-work root or logical identity is malformed".to_owned(),
        ));
    }
    let key = work_index_key(work_id)?;
    let empty = work_index_empty_hashes();
    let mut current = root.to_owned();
    let mut siblings_root_to_leaf = Vec::with_capacity(WORK_INDEX_DEPTH);
    let mut depth = 0_usize;
    while depth < WORK_INDEX_DEPTH {
        if current == empty[depth] {
            current.clone_from(&empty[WORK_INDEX_DEPTH]);
            break;
        }
        let node = load(&current)?.ok_or_else(|| ProtocolError::NotFound {
            message: format!("Virtual archived-work index node {current} is missing"),
        })?;
        if node.identity()? != current {
            return Err(virtual_integrity(
                "virtual_archive_work_branch_identity_mismatch",
                "Virtual archived-work resolver returned the wrong branch",
            ));
        }
        let VirtualArchiveWorkIndexNode::Branch {
            depth: node_depth,
            left,
            right,
            ..
        } = node
        else {
            return Err(virtual_integrity(
                "virtual_archive_work_branch_shape_mismatch",
                "Virtual archived-work resolver reached a member before depth 256",
            ));
        };
        if usize::from(node_depth) != depth {
            return Err(virtual_integrity(
                "virtual_archive_work_branch_depth_mismatch",
                "Virtual archived-work branch has the wrong depth",
            ));
        }
        if sparse_index_bit(&key, depth) {
            siblings_root_to_leaf.push(left);
            current = right;
        } else {
            siblings_root_to_leaf.push(right);
            current = left;
        }
        depth += 1;
    }
    let value = if current == empty[WORK_INDEX_DEPTH] {
        None
    } else {
        let node = load(&current)?.ok_or_else(|| ProtocolError::NotFound {
            message: format!("Virtual archived-work index member {current} is missing"),
        })?;
        if node.identity()? != current {
            return Err(virtual_integrity(
                "virtual_archive_work_member_identity_mismatch",
                "Virtual archived-work resolver returned the wrong member",
            ));
        }
        let VirtualArchiveWorkIndexNode::Member {
            key_hash, value, ..
        } = node
        else {
            return Err(virtual_integrity(
                "virtual_archive_work_member_shape_mismatch",
                "Virtual archived-work resolver reached a branch at depth 256",
            ));
        };
        if key_hash != sparse_index_key_id(&key) || value.work_id != work_id {
            return Err(virtual_integrity(
                "virtual_archive_work_key_collision",
                "Virtual archived-work key collision changed logical identity",
            ));
        }
        Some(value)
    };
    siblings_root_to_leaf.reverse();
    let proof = VirtualArchiveWorkProof {
        proof_version: WORK_INDEX_PROOF_VERSION.to_owned(),
        work_id: work_id.to_owned(),
        empty_depth: value.is_none().then_some(
            u16::try_from(depth).map_err(|error| ProtocolError::Validation(error.to_string()))?,
        ),
        value,
        siblings: siblings_root_to_leaf,
    };
    proof.verify(root)?;
    Ok(proof)
}

/// Build and verify immutable nodes for one absent archived-work key.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn build_virtual_work_index_update(
    parent_root_digest: &str,
    proof: VirtualArchiveWorkProof,
    value: &ArchivedWorkIndex,
) -> ProtocolResult<(
    VirtualArchiveWorkIndexUpdate,
    Vec<VirtualArchiveWorkIndexNode>,
)> {
    proof.verify(parent_root_digest)?;
    if proof.work_id != value.work_id || proof.value.is_some() {
        return Err(ProtocolError::IllegalTransition(format!(
            "logical work {} already has archived identity authority",
            value.work_id
        )));
    }
    verify_archived_work_index(value)?;
    let proof_depth = proof.validate_shape()?;
    let result_root_digest = proof.root_with_value(proof_depth, Some(value))?;
    let key = work_index_key(&value.work_id)?;
    let empty = work_index_empty_hashes();
    let member_id = work_index_member_leaf(&key, value)?;
    let mut nodes = vec![VirtualArchiveWorkIndexNode::Member {
        node_id: member_id.clone(),
        key_hash: sparse_index_key_id(&key),
        value: value.clone(),
    }];
    let mut current = member_id;
    for level in 0..WORK_INDEX_DEPTH - proof_depth {
        let depth = WORK_INDEX_DEPTH - level - 1;
        let sibling = &empty[WORK_INDEX_DEPTH - level];
        let (left, right) = if sparse_index_bit(&key, depth) {
            (sibling.clone(), current.clone())
        } else {
            (current.clone(), sibling.clone())
        };
        current = work_index_node(depth, &left, &right)?;
        nodes.push(VirtualArchiveWorkIndexNode::Branch {
            node_id: current.clone(),
            depth: u16::try_from(depth)
                .map_err(|error| ProtocolError::Validation(error.to_string()))?,
            left,
            right,
        });
    }
    for (level, sibling) in proof.siblings.iter().enumerate() {
        let depth = proof_depth - level - 1;
        let (left, right) = if sparse_index_bit(&key, depth) {
            (sibling.clone(), current.clone())
        } else {
            (current.clone(), sibling.clone())
        };
        current = work_index_node(depth, &left, &right)?;
        nodes.push(VirtualArchiveWorkIndexNode::Branch {
            node_id: current.clone(),
            depth: u16::try_from(depth)
                .map_err(|error| ProtocolError::Validation(error.to_string()))?,
            left,
            right,
        });
    }
    if current != result_root_digest {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archived-work nodes do not reach their result root".to_owned(),
        ));
    }
    Ok((
        VirtualArchiveWorkIndexUpdate {
            parent_root_digest: parent_root_digest.to_owned(),
            nonmembership: proof,
            value: value.clone(),
            result_root_digest,
        },
        nodes,
    ))
}

impl VirtualArchiveCommandIndexNode {
    /// Verify and return this immutable locator node's content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn identity(&self) -> ProtocolResult<&str> {
        match self {
            Self::Branch {
                node_id,
                depth,
                left,
                right,
            } => {
                let depth = usize::from(*depth);
                if depth >= COMMAND_INDEX_DEPTH
                    || !valid_content_id(left)
                    || !valid_content_id(right)
                    || command_index_node(depth, left, right)? != *node_id
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual archived-command locator branch is malformed".to_owned(),
                    ));
                }
                Ok(node_id)
            }
            Self::Member {
                node_id,
                key_hash,
                value,
            } => {
                verify_archived_command_index(value)?;
                let key = command_index_key(&value.journal_id, &value.command_id)?;
                if key_hash != &command_index_key_id(&key)
                    || command_index_member_leaf(&key, value)? != *node_id
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual archived-command locator member is malformed".to_owned(),
                    ));
                }
                Ok(node_id)
            }
        }
    }
}

impl VirtualArchiveCommandIndexProof {
    /// Verify this membership or non-membership proof against one exact root.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self, expected_root: &str) -> ProtocolResult<()> {
        let depth = self.validate_shape()?;
        if !valid_content_id(expected_root) || self.root(depth)? != expected_root {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual archived-command locator proof does not reach its expected root"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> ProtocolResult<usize> {
        if self.proof_version != COMMAND_INDEX_PROOF_VERSION
            || validate_identity("Virtual archived-command journal", &self.journal_id).is_err()
            || validate_identity("Virtual archived command", &self.command_id).is_err()
            || self.siblings.iter().any(|value| !valid_content_id(value))
        {
            return Err(ProtocolError::Validation(
                "Virtual archived-command locator proof version, key, or sibling is malformed"
                    .to_owned(),
            ));
        }
        let depth = match (&self.value, self.empty_depth) {
            (Some(value), None) if self.siblings.len() == COMMAND_INDEX_DEPTH => {
                verify_archived_command_index(value)?;
                if value.journal_id != self.journal_id || value.command_id != self.command_id {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual archived-command locator membership changed its exact key"
                            .to_owned(),
                    ));
                }
                COMMAND_INDEX_DEPTH
            }
            (None, Some(depth))
                if usize::from(depth) <= COMMAND_INDEX_DEPTH
                    && self.siblings.len() == usize::from(depth) =>
            {
                usize::from(depth)
            }
            _ => {
                return Err(ProtocolError::Validation(
                    "Virtual archived-command locator proof has an unsupported membership shape"
                        .to_owned(),
                ));
            }
        };
        if self.value.is_none()
            && depth > 0
            && self.siblings.first() == command_index_empty_hashes().get(depth)
        {
            return Err(ProtocolError::Validation(
                "Virtual archived-command locator non-membership proof is not maximally compressed"
                    .to_owned(),
            ));
        }
        Ok(depth)
    }

    fn root(&self, depth: usize) -> ProtocolResult<String> {
        self.root_with_value(depth, self.value.as_ref())
    }

    fn root_with_value(
        &self,
        proof_depth: usize,
        value: Option<&ArchivedCommandIndex>,
    ) -> ProtocolResult<String> {
        let key = command_index_key(&self.journal_id, &self.command_id)?;
        let empty = command_index_empty_hashes();
        let mut current = match (&self.value, value) {
            (Some(expected), Some(value)) if expected == value => {
                command_index_member_leaf(&key, value)?
            }
            (Some(_), _) => {
                return Err(ProtocolError::Validation(
                    "Virtual archived-command locator membership value cannot be replaced"
                        .to_owned(),
                ));
            }
            (None, Some(value)) => {
                verify_archived_command_index(value)?;
                if value.journal_id != self.journal_id || value.command_id != self.command_id {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual archived-command locator insertion changed its exact key"
                            .to_owned(),
                    ));
                }
                let mut current = command_index_member_leaf(&key, value)?;
                for level in 0..COMMAND_INDEX_DEPTH - proof_depth {
                    let depth = COMMAND_INDEX_DEPTH - level - 1;
                    let sibling = &empty[COMMAND_INDEX_DEPTH - level];
                    current = if sparse_index_bit(&key, depth) {
                        command_index_node(depth, sibling, &current)?
                    } else {
                        command_index_node(depth, &current, sibling)?
                    };
                }
                current
            }
            (None, None) => empty[proof_depth].clone(),
        };
        for (level, sibling) in self.siblings.iter().enumerate() {
            let depth = proof_depth - level - 1;
            current = if sparse_index_bit(&key, depth) {
                command_index_node(depth, sibling, &current)?
            } else {
                command_index_node(depth, &current, sibling)?
            };
        }
        Ok(current)
    }
}

/// Return the canonical empty cumulative archived-command locator root.
pub fn virtual_command_index_empty_root() -> String {
    command_index_empty_hashes()[0].clone()
}

/// Resolve one fixed-depth locator proof from immutable content-addressed nodes.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn resolve_virtual_command_index_proof(
    root: &str,
    journal_id: &str,
    command_id: &str,
    mut load: impl FnMut(&str) -> ProtocolResult<Option<VirtualArchiveCommandIndexNode>>,
) -> ProtocolResult<VirtualArchiveCommandIndexProof> {
    validate_virtual_command_index_request(root, journal_id, command_id)?;
    let key = command_index_key(journal_id, command_id)?;
    let empty = command_index_empty_hashes();
    let mut current = root.to_owned();
    let mut siblings_root_to_leaf = Vec::with_capacity(COMMAND_INDEX_DEPTH);
    let mut depth = 0_usize;
    while depth < COMMAND_INDEX_DEPTH {
        if current == empty[depth] {
            current.clone_from(&empty[COMMAND_INDEX_DEPTH]);
            break;
        }
        let node = load(&current)?.ok_or_else(|| ProtocolError::NotFound {
            message: format!("Virtual archived-command locator node {current} is missing"),
        })?;
        if node.identity()? != current {
            return Err(virtual_integrity(
                "virtual_archive_command_branch_identity_mismatch",
                "Virtual archived-command locator resolver returned the wrong branch",
            ));
        }
        let VirtualArchiveCommandIndexNode::Branch {
            depth: node_depth,
            left,
            right,
            ..
        } = node
        else {
            return Err(virtual_integrity(
                "virtual_archive_command_branch_shape_mismatch",
                "Virtual archived-command locator reached a member before depth 256",
            ));
        };
        if usize::from(node_depth) != depth {
            return Err(virtual_integrity(
                "virtual_archive_command_branch_depth_mismatch",
                "Virtual archived-command locator branch has the wrong depth",
            ));
        }
        if sparse_index_bit(&key, depth) {
            siblings_root_to_leaf.push(left);
            current = right;
        } else {
            siblings_root_to_leaf.push(right);
            current = left;
        }
        depth += 1;
    }
    let value = if current == empty[COMMAND_INDEX_DEPTH] {
        None
    } else {
        let node = load(&current)?.ok_or_else(|| ProtocolError::NotFound {
            message: format!("Virtual archived-command locator member {current} is missing"),
        })?;
        if node.identity()? != current {
            return Err(virtual_integrity(
                "virtual_archive_command_member_identity_mismatch",
                "Virtual archived-command locator resolver returned the wrong member",
            ));
        }
        let VirtualArchiveCommandIndexNode::Member {
            key_hash, value, ..
        } = node
        else {
            return Err(virtual_integrity(
                "virtual_archive_command_member_shape_mismatch",
                "Virtual archived-command locator reached a branch at depth 256",
            ));
        };
        if key_hash != command_index_key_id(&key)
            || value.journal_id != journal_id
            || value.command_id != command_id
        {
            return Err(virtual_integrity(
                "virtual_archive_command_key_collision",
                "Virtual archived-command locator key collision changed exact identity",
            ));
        }
        Some(value)
    };
    finish_virtual_command_index_proof(
        root,
        journal_id,
        command_id,
        depth,
        value,
        siblings_root_to_leaf,
    )
}

fn validate_virtual_command_index_request(
    root: &str,
    journal_id: &str,
    command_id: &str,
) -> ProtocolResult<()> {
    if !valid_content_id(root)
        || validate_identity("Virtual archived-command journal", journal_id).is_err()
        || validate_identity("Virtual archived command", command_id).is_err()
    {
        return Err(ProtocolError::Validation(
            "Virtual archived-command locator root or exact key is malformed".to_owned(),
        ));
    }
    Ok(())
}

fn finish_virtual_command_index_proof(
    root: &str,
    journal_id: &str,
    command_id: &str,
    depth: usize,
    value: Option<ArchivedCommandIndex>,
    mut siblings: Vec<String>,
) -> ProtocolResult<VirtualArchiveCommandIndexProof> {
    siblings.reverse();
    let proof = VirtualArchiveCommandIndexProof {
        proof_version: COMMAND_INDEX_PROOF_VERSION.to_owned(),
        journal_id: journal_id.to_owned(),
        command_id: command_id.to_owned(),
        empty_depth: value.is_none().then_some(
            u16::try_from(depth).map_err(|error| ProtocolError::Validation(error.to_string()))?,
        ),
        value,
        siblings,
    };
    proof.verify(root)?;
    Ok(proof)
}

/// Build and verify immutable nodes for one absent archived-command key.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn build_virtual_command_index_update(
    parent_root_digest: &str,
    proof: VirtualArchiveCommandIndexProof,
    value: &ArchivedCommandIndex,
) -> ProtocolResult<(
    VirtualArchiveCommandIndexUpdate,
    Vec<VirtualArchiveCommandIndexNode>,
)> {
    proof.verify(parent_root_digest)?;
    if proof.journal_id != value.journal_id
        || proof.command_id != value.command_id
        || proof.value.is_some()
    {
        return Err(ProtocolError::IllegalTransition(format!(
            "command {} in journal {} already has archived locator authority",
            value.command_id, value.journal_id
        )));
    }
    verify_archived_command_index(value)?;
    let proof_depth = proof.validate_shape()?;
    let result_root_digest = proof.root_with_value(proof_depth, Some(value))?;
    let key = command_index_key(&value.journal_id, &value.command_id)?;
    let empty = command_index_empty_hashes();
    let member_id = command_index_member_leaf(&key, value)?;
    let mut nodes = vec![VirtualArchiveCommandIndexNode::Member {
        node_id: member_id.clone(),
        key_hash: command_index_key_id(&key),
        value: value.clone(),
    }];
    let mut current = member_id;
    for level in 0..COMMAND_INDEX_DEPTH - proof_depth {
        let depth = COMMAND_INDEX_DEPTH - level - 1;
        let sibling = &empty[COMMAND_INDEX_DEPTH - level];
        let (left, right) = if sparse_index_bit(&key, depth) {
            (sibling.clone(), current.clone())
        } else {
            (current.clone(), sibling.clone())
        };
        current = command_index_node(depth, &left, &right)?;
        nodes.push(VirtualArchiveCommandIndexNode::Branch {
            node_id: current.clone(),
            depth: u16::try_from(depth)
                .map_err(|error| ProtocolError::Validation(error.to_string()))?,
            left,
            right,
        });
    }
    for (level, sibling) in proof.siblings.iter().enumerate() {
        let depth = proof_depth - level - 1;
        let (left, right) = if sparse_index_bit(&key, depth) {
            (sibling.clone(), current.clone())
        } else {
            (current.clone(), sibling.clone())
        };
        current = command_index_node(depth, &left, &right)?;
        nodes.push(VirtualArchiveCommandIndexNode::Branch {
            node_id: current.clone(),
            depth: u16::try_from(depth)
                .map_err(|error| ProtocolError::Validation(error.to_string()))?,
            left,
            right,
        });
    }
    if current != result_root_digest {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archived-command locator nodes do not reach their result root".to_owned(),
        ));
    }
    Ok((
        VirtualArchiveCommandIndexUpdate {
            parent_root_digest: parent_root_digest.to_owned(),
            nonmembership: proof,
            value: value.clone(),
            result_root_digest,
        },
        nodes,
    ))
}

impl VirtualArchiveManifest {
    /// Verify the complete typed cold payload and cumulative work-index chain.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        verify_virtual_archive_header(self)?;
        verify_virtual_archive_work(self)?;
        verify_virtual_archive_receipts(self)?;
        if cymule_core::canonical_bytes(self)?.len() > MAX_VIRTUAL_ARCHIVE_BYTES {
            return Err(ProtocolError::Validation(
                "Virtual archive exceeds the bounded provider object contract".to_owned(),
            ));
        }
        Ok(())
    }
}

fn verify_virtual_archive_header(manifest: &VirtualArchiveManifest) -> ProtocolResult<()> {
    if manifest.manifest_version != VIRTUAL_ARCHIVE_MANIFEST_VERSION {
        return Err(ProtocolError::Validation(
            "unsupported Virtual archive manifest version".to_owned(),
        ));
    }
    validate_identity("Virtual archive region", &manifest.region_id)?;
    validate_identity("Virtual archive Run", &manifest.run_id)?;
    if manifest.source_causal_cut.is_empty() {
        return Err(ProtocolError::Validation(
            "Virtual archive requires a non-empty causal cut".to_owned(),
        ));
    }
    for checkpoint_id in &manifest.source_causal_cut {
        validate_identity("Virtual archive causal checkpoint", checkpoint_id)?;
    }
    validate_content_id(
        "Virtual archived-work parent root",
        &manifest.parent_work_index_root_digest,
    )?;
    validate_content_id(
        "Virtual archived-work result root",
        &manifest.result_work_index_root_digest,
    )?;
    if manifest.occurrences.is_empty()
        || manifest.occurrences.len() > MAX_VIRTUAL_MUTATION_ITEMS
        || manifest.work_index.is_empty()
        || manifest.work_index.len() > MAX_VIRTUAL_MUTATION_ITEMS
        || manifest.command_receipts.len() > MAX_VIRTUAL_MUTATION_ITEMS
    {
        return Err(ProtocolError::Validation(
            "Virtual archive requires bounded non-empty work and occurrence history".to_owned(),
        ));
    }
    Ok(())
}

fn verify_virtual_archive_work(manifest: &VirtualArchiveManifest) -> ProtocolResult<()> {
    let mut expected_work = BTreeMap::<String, ArchivedWorkIndex>::new();
    for (occurrence_id, occurrence) in &manifest.occurrences {
        occurrence.verify()?;
        if occurrence.occurrence_id != *occurrence_id
            || occurrence.region_id != manifest.region_id
            || occurrence.run_id != manifest.run_id
            || !matches!(
                occurrence.state,
                WorkOccurrenceState::Succeeded
                    | WorkOccurrenceState::Failed
                    | WorkOccurrenceState::Cancelled
            )
        {
            return Err(ProtocolError::IllegalTransition(
                "Virtual archive contains a nonterminal or cross-region occurrence".to_owned(),
            ));
        }
        let value = ArchivedWorkIndex {
            work_id: occurrence.work_id.clone(),
            region_id: occurrence.region_id.clone(),
            run_id: occurrence.run_id.clone(),
            occurrence_id: occurrence.occurrence_id.clone(),
            max_epoch: occurrence.epoch,
            terminal_state: occurrence.state,
        };
        match expected_work.get(&occurrence.work_id) {
            Some(previous) if previous.max_epoch >= occurrence.epoch => {}
            _ => {
                expected_work.insert(occurrence.work_id.clone(), value);
            }
        }
    }
    if expected_work != manifest.work_index
        || manifest.work_index_updates.len() != manifest.work_index.len()
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive work index does not equal its greatest terminal fences".to_owned(),
        ));
    }
    let mut root = manifest.parent_work_index_root_digest.clone();
    for ((work_id, value), update) in manifest.work_index.iter().zip(&manifest.work_index_updates) {
        let (expected, nodes) =
            build_virtual_work_index_update(&root, update.nonmembership.clone(), value)?;
        if nodes.is_empty()
            || update != &expected
            || update.value.work_id != *work_id
            || update.parent_root_digest != root
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual archive work-index update changed its exact insertion chain".to_owned(),
            ));
        }
        root.clone_from(&update.result_root_digest);
    }
    if root != manifest.result_work_index_root_digest {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive work-index updates do not reach their result root".to_owned(),
        ));
    }

    Ok(())
}

fn verify_virtual_archive_receipts(manifest: &VirtualArchiveManifest) -> ProtocolResult<()> {
    match (&manifest.journal_id, manifest.command_receipts.is_empty()) {
        (None, true) => {}
        (Some(journal_id), false) => {
            validate_content_id("Virtual scheduler journal", journal_id)?;
        }
        _ => {
            return Err(ProtocolError::IllegalTransition(
                "Virtual archive journal presence and typed receipt set disagree".to_owned(),
            ));
        }
    }
    let mut receipt_scheduler = None::<&str>;
    for (command_id, receipt) in &manifest.command_receipts {
        receipt.verify()?;
        if receipt.command.command_id() != command_id {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual archive typed receipt changed its semantic command identity".to_owned(),
            ));
        }
        if receipt_scheduler
            .is_some_and(|scheduler_id| scheduler_id != receipt.command.scheduler_id())
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual archive typed receipts span scheduler authorities".to_owned(),
            ));
        }
        receipt_scheduler = Some(receipt.command.scheduler_id());
    }
    if let (Some(journal_id), Some(scheduler_id)) = (&manifest.journal_id, receipt_scheduler)
        && journal_id != &virtual_scheduler_journal_id(scheduler_id)?
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive journal does not derive from its typed receipts".to_owned(),
        ));
    }
    Ok(())
}

/// Derive the certificate-bound occurrence and command Merkle roots for one
/// exact typed archive manifest.
///
/// This function is pure. Durable uses it after the pinned archive provider
/// publishes the same manifest bytes; callers cannot submit either root as
/// persistence authority.
///
/// # Errors
///
/// Returns an error when the manifest or its exact typed Merkle leaves are
/// malformed.
pub fn virtual_archive_roots(
    manifest: &VirtualArchiveManifest,
) -> ProtocolResult<(String, Option<String>)> {
    manifest.verify()?;
    Ok((
        virtual_archive_occurrence_root(&manifest.occurrences)?,
        virtual_archive_command_root(manifest.journal_id.as_deref(), &manifest.command_receipts)?,
    ))
}

/// Canonically encode one typed archive and derive every descriptor-scoped
/// bounded range proof without invoking a provider.
///
/// # Errors
///
/// Returns an error when the manifest, canonical byte layout, typed ranges,
/// or recomputed Merkle roots disagree.
pub fn build_virtual_archive_layout(
    manifest: &VirtualArchiveManifest,
) -> ProtocolResult<VirtualArchiveLayout> {
    manifest.verify()?;
    let bytes = cymule_core::canonical_bytes(manifest)?;
    let occurrence_ranges = build_virtual_archive_range_proofs(
        &bytes,
        "occurrences",
        &manifest.occurrences,
        |occurrence_id, digest| {
            cymule_core::content_id(OCCURRENCE_LEAF_DOMAIN, &(occurrence_id, digest))
                .map_err(ProtocolError::from)
        },
        OCCURRENCE_NODE_DOMAIN,
    )?
    .ok_or_else(|| {
        ProtocolError::Validation("Virtual archive contains no occurrence ranges".to_owned())
    })?;
    let occurrence_root_digest = occurrence_ranges.0;
    let occurrence_proofs = occurrence_ranges
        .1
        .into_iter()
        .map(|(occurrence_id, proof)| {
            (
                occurrence_id.clone(),
                VirtualArchiveOccurrenceProof {
                    occurrence_id,
                    index: proof.index,
                    offset: proof.offset,
                    length: proof.length,
                    digest: proof.digest,
                    path: proof.path,
                },
            )
        })
        .collect();
    let (command_root_digest, command_proofs) =
        build_virtual_archive_command_layout(manifest, &bytes)?;
    let (expected_occurrence_root, expected_command_root) = virtual_archive_roots(manifest)?;
    if occurrence_root_digest != expected_occurrence_root
        || command_root_digest != expected_command_root
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive layout changed the framework Merkle roots".to_owned(),
        ));
    }
    Ok(VirtualArchiveLayout {
        bytes,
        occurrence_root_digest,
        occurrence_proofs,
        command_root_digest,
        command_proofs,
    })
}

fn build_virtual_archive_command_layout(
    manifest: &VirtualArchiveManifest,
    bytes: &[u8],
) -> ProtocolResult<(Option<String>, BTreeMap<String, VirtualArchiveCommandProof>)> {
    if manifest.command_receipts.is_empty() {
        return Ok((None, BTreeMap::new()));
    }
    let journal_id = manifest.journal_id.as_deref().ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Virtual archived receipts require their derived journal".to_owned(),
        )
    })?;
    let Some((root, proofs)) = build_virtual_archive_range_proofs(
        bytes,
        "command_receipts",
        &manifest.command_receipts,
        |command_id, digest| {
            cymule_core::content_id(COMMAND_LEAF_DOMAIN, &(journal_id, command_id, digest))
                .map_err(ProtocolError::from)
        },
        COMMAND_NODE_DOMAIN,
    )?
    else {
        return Err(ProtocolError::IllegalTransition(
            "Virtual archived receipt ranges lost their derived journal".to_owned(),
        ));
    };
    let proofs = proofs
        .into_iter()
        .map(|(command_id, proof)| {
            (
                command_id.clone(),
                VirtualArchiveCommandProof {
                    journal_id: journal_id.to_owned(),
                    command_id,
                    index: proof.index,
                    offset: proof.offset,
                    length: proof.length,
                    digest: proof.digest,
                    path: proof.path,
                },
            )
        })
        .collect();
    Ok((Some(root), proofs))
}

/// Bounded completion projection authenticated by a compaction certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCompletionSummary {
    /// Region summarized by this projection.
    pub region_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Exact archived occurrence count.
    pub occurrence_count: u64,
    /// Exact completed logical-work count.
    pub work_count: u64,
    /// Logical work ending successfully.
    pub succeeded_count: u64,
    /// Logical work ending in terminal failure.
    pub failed_count: u64,
    /// Logical work ending by cancellation.
    pub cancelled_count: u64,
    /// Digest of terminal result Artifact references.
    pub output_digest: String,
    /// Digest of failure and cancellation evidence references.
    pub evidence_digest: String,
    /// Digest of the retained logical-work debug index.
    pub retained_debug_index_digest: String,
}

/// Verified witness that exact completed occurrence history moved to an archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCompactionCertificate {
    /// Certificate schema and semantic version.
    pub certificate_version: String,
    /// Content identity of every certificate field except this identity.
    pub certificate_id: String,
    /// Causally closed checkpoint cut represented by the summary.
    pub source_causal_cut: BTreeSet<String>,
    /// Bounded completed-state projection.
    pub summary: VirtualCompletionSummary,
    /// Digest of the complete archive manifest.
    pub summary_state_digest: String,
    /// Merkle root authenticating bounded occurrence range proofs.
    pub occurrence_root_digest: String,
    /// Cumulative archived-work root before this compaction.
    pub parent_work_index_root_digest: String,
    /// Digest of the exact sequential work-index update set retained cold.
    pub work_index_updates_digest: String,
    /// Cumulative archived-work root after this compaction.
    pub work_index_root_digest: String,
    /// Merkle root authenticating archived command-receipt range proofs.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub command_root_digest: Option<String>,
    /// Exact archived command-receipt count.
    pub command_count: u64,
    /// Unresolved external obligations retained outside this completed subtree.
    pub unresolved_obligations: BTreeSet<String>,
    /// Exact `ExecutionBinding` Artifacts required to interpret archived history.
    pub retained_execution_bindings: BTreeSet<ArtifactRef>,
    /// Replay capability after this retention decision.
    pub replay_availability: ReplayAvailability,
    /// Semantic descriptor for cold exact history used by partial rehydration.
    pub rehydration_manifest: ResourceHandle,
    /// Pinned archive generation that owns the object and cumulative indexes.
    pub archive: VirtualArchiveBinding,
}

/// Idempotent request to compact one completed virtual region.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCompactionCommand {
    /// Control schema and semantic version.
    pub control_version: String,
    /// Stable caller-generated command identity.
    pub command_id: String,
    /// Completed region to move cold.
    pub region_id: String,
    /// Causally closed durable checkpoint cut covered by the archive.
    pub source_causal_cut: BTreeSet<String>,
    /// Complete bounded hot work set removed by this compaction.
    pub work_ids: BTreeSet<String>,
    /// Complete bounded hot occurrence set removed by this compaction.
    pub occurrence_ids: BTreeSet<String>,
    /// Complete bounded typed command-receipt set moved cold.
    pub archived_command_ids: BTreeSet<String>,
    /// Pinned archive generation fixed by scheduler initialization.
    pub archive: VirtualArchiveBinding,
}

/// Durable receipt for one compacted region.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCompactionReceipt {
    /// Exact admitted command.
    pub command: VirtualCompactionCommand,
    /// Verified resulting certificate.
    pub certificate: VirtualCompactionCertificate,
    /// Exact Resource archive pin introduced by the same outer Virtual CAS.
    pub resource_pin: ResourcePinReceipt,
    /// Cumulative archived-command root before this compaction.
    pub parent_command_index_root_digest: String,
    /// Digest of the exact sequential command-locator insertions retained cold.
    pub command_index_updates_digest: String,
    /// Cumulative archived-command root after this compaction.
    pub command_index_root_digest: String,
}

/// Idempotent command retiring one immutable cold-history certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveRetirementCommand {
    /// Control schema and semantic version.
    pub control_version: String,
    /// Content-derived identity of this complete command.
    pub command_id: String,
    /// Exact immutable certificate whose Resource retention is terminated.
    pub certificate_id: String,
}

impl VirtualArchiveRetirementCommand {
    /// Construct and seal one exact retirement command.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn new(certificate_id: impl Into<String>) -> ProtocolResult<Self> {
        let certificate_id = certificate_id.into();
        validate_content_id("Virtual compaction certificate", &certificate_id)?;
        let command_id =
            cymule_core::content_id(VIRTUAL_ARCHIVE_RETIREMENT_CONTROL_VERSION, &certificate_id)?;
        Ok(Self {
            control_version: VIRTUAL_ARCHIVE_RETIREMENT_CONTROL_VERSION.to_owned(),
            command_id,
            certificate_id,
        })
    }

    /// Verify the command generation and its complete content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.control_version != VIRTUAL_ARCHIVE_RETIREMENT_CONTROL_VERSION {
            return Err(ProtocolError::Validation(format!(
                "unsupported Virtual archive retirement control version {}",
                self.control_version
            )));
        }
        validate_content_id("Virtual compaction certificate", &self.certificate_id)?;
        let expected = cymule_core::content_id(
            VIRTUAL_ARCHIVE_RETIREMENT_CONTROL_VERSION,
            &self.certificate_id,
        )?;
        if self.command_id != expected {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual archive retirement command identity does not match its certificate"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Derive the only Resource archive-release delta this command may own.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn release(
        &self,
        pin: &crate::resource::ResourcePin,
    ) -> ProtocolResult<ResourceArchiveRelease> {
        self.verify()?;
        ResourceArchiveRelease::new(self.command_id.clone(), pin)
            .map_err(|error| ProtocolError::Validation(error.to_string()))
    }
}

/// Terminal evidence that one certificate and its permanent archive pin were
/// retired in the same M1 compare-and-swap transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveRetirementReceipt {
    /// Exact admitted retirement command.
    pub command: VirtualArchiveRetirementCommand,
    /// Exact Resource archive-pin release committed by the same transition.
    pub resource_release: ResourceReleaseReceipt,
}

impl VirtualArchiveRetirementReceipt {
    /// Verify cross-domain identity binding without reading mutable state.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        self.command.verify()?;
        self.resource_release
            .verify()
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        let release = self.command.release(&self.resource_release.pin)?;
        if self.resource_release.command_id != self.command.command_id
            || self.resource_release.release_id != release.release_id
            || self.resource_release.pin.pin_id != release.pin_id
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual archive retirement receipt does not bind its exact Resource release"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// Idempotent request to restore selected exact occurrence records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRehydrationCommand {
    /// Control schema and semantic version.
    pub control_version: String,
    /// Stable caller-generated command identity.
    pub command_id: String,
    /// Certificate whose manifest is authoritative.
    pub certificate_id: String,
    /// Exact occurrence identities to restore into the hot projection.
    pub occurrence_ids: BTreeSet<String>,
}

/// Durable partial rehydration receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRehydrationReceipt {
    /// Exact admitted command.
    pub command: VirtualRehydrationCommand,
    /// Occurrence identities restored or already present identically.
    pub restored_occurrence_ids: BTreeSet<String>,
}

/// Materialization and active-work bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontierLimits {
    /// Global ready plus active bound.
    pub max_materialized: usize,
    /// Active claims allowed globally.
    pub max_active: usize,
    /// Active claims allowed per Run.
    pub max_active_per_run: usize,
    /// Maximum source page requested at once.
    pub materialize_batch: usize,
}

/// Deterministic cost-fairness and priority-aging policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulingPolicy {
    /// Deficit quantum granted per Run weight unit.
    pub base_quantum: u64,
    /// Successful dispatches required for one priority-aging step.
    pub aging_interval: u64,
}

impl Default for SchedulingPolicy {
    fn default() -> Self {
        Self {
            base_quantum: 1,
            aging_interval: 1,
        }
    }
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Closed lease intent consumed by Durable from current M1 authority. Callers
/// never submit a precomputed lease epoch or fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualLeaseIntent {
    /// Exact coordination resource being acquired or renewed.
    pub resource: String,
    /// Exact owner receiving the resulting fence.
    pub owner: String,
    /// Current-head Clock reference under which Durable derives the lease.
    pub clock: ClockObservationRef,
    /// Positive logical lease duration.
    pub ttl: u64,
}

impl VirtualLeaseIntent {
    /// Verify the closed lease input without accepting a caller-authored epoch.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Virtual lease resource", &self.resource)?;
        validate_identity("Virtual lease owner", &self.owner)?;
        self.clock.verify()?;
        if self.ttl == 0 || self.ttl > cymule_core::MAX_EXACT_INTEGER {
            return Err(ProtocolError::Validation(
                "Virtual lease TTL must use the exact positive integer range".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Semantic genesis of one independently persisted virtual scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualInitializationCommand {
    /// Initialization command generation.
    pub control_version: String,
    /// Stable semantic scheduler namespace. Its physical journal is derived.
    pub scheduler_id: String,
    /// Stable idempotency identity of this exact initialization.
    pub command_id: String,
    /// Frozen scheduler frontier bounds.
    pub limits: FrontierLimits,
    /// Frozen deterministic fairness policy.
    pub scheduling_policy: SchedulingPolicy,
    /// Immutable archive/index provider generation for this scheduler.
    pub archive: VirtualArchiveBinding,
    /// Complete initial region set.
    pub regions: Vec<VirtualRegion>,
    /// Strictly Run-identity ordered complete execution configuration.
    pub runs: Vec<VirtualRunDefinition>,
    /// Exact bytes for every distinct region source Artifact.
    pub source_artifacts: Vec<ArtifactRecord>,
}

/// Semantic request to materialize the reducer-selected next region.
///
/// The provider page and cold-index proofs are intentionally absent. Durable
/// obtains them from the exact binding-pinned provider and places them only in
/// non-serializable reduction authority plus the resulting replay receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualMaterializationCommand {
    /// Materialization command generation.
    pub control_version: String,
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Stable idempotency identity of this materialization intent.
    pub command_id: String,
    /// Reducer-selected region; direct callers cannot skip the fairness head.
    pub region_id: String,
    /// Exact adapter generation observed before provider I/O.
    pub expected_source: RegionSourceBinding,
    /// Exact cursor observed before provider I/O.
    pub expected_cursor: VirtualCursor,
}

/// Semantic request to consume one already-admitted M1 activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualActivationCommand {
    /// Activation-consumption command generation.
    pub control_version: String,
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Content identity of this exact scheduler and M1 activation pair.
    pub command_id: String,
    /// Exact M1 activation identity resolved by Durable.
    pub activation_id: String,
}

/// One normal fenced resolution plus its exact optional Artifact product.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualResolutionPersistenceCommand {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Complete normal work-resolution command.
    pub command: WorkResolutionCommand,
    /// Exact result, failure, or cancellation bytes; absent for Parked.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub artifact: Option<ArtifactRecord>,
}

/// Semantic request for one binding-pinned topology migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualMigrationPersistenceCommand {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Stable idempotency identity of this migration request.
    pub command_id: String,
    /// Complete scalar request sent to the binding-pinned migrator.
    pub request: RegionMigrationRequest,
}

/// Framework-derived immutable archive publication admitted by compaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCompactionPublication {
    /// Exact provider publication for the certificate's archive Resource.
    pub publication: ResourcePublication,
    /// Merkle root recomputed from the exact occurrence section.
    pub occurrence_root_digest: String,
    /// Merkle root recomputed from exact archived receipts, when present.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub command_root_digest: Option<String>,
    /// Ordered cumulative archived-work insertions used by the certificate.
    pub work_index_updates: Vec<VirtualArchiveWorkIndexUpdate>,
    /// Ordered cumulative archived-command locator insertions used by the receipt.
    pub command_index_updates: Vec<VirtualArchiveCommandIndexUpdate>,
}

/// One cold-history publication request with no caller-authored checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCompactionPersistenceCommand {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Complete compaction intent.
    pub command: VirtualCompactionCommand,
}

/// One exact occurrence and its certificate-bound immutable range proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRehydratedOccurrence {
    /// Complete canonical occurrence value.
    pub occurrence: WorkOccurrence,
    /// Exact range/Merkle proof for that value.
    pub proof: VirtualArchiveOccurrenceProof,
}

/// One bounded cold-history read result admitted by partial rehydration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRehydrationPersistenceCommand {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Complete exact occurrence selection.
    pub command: VirtualRehydrationCommand,
}

/// Exact provider/M1 evidence retained only in the result receipt.
///
/// Durable never accepts this enum as a commit input. It constructs the value
/// after resolving M1 authority or invoking the exact binding-pinned provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "evidence",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum VirtualPersistenceEvidence {
    /// The semantic operation introduces no provider/M1 product.
    None,
    /// Exact page and cumulative absence proofs returned by a `RegionSource`.
    Materialized {
        /// Bounded provider page.
        page: MaterializedPage,
        /// Exact cold non-membership proof per returned work identity.
        archived_work_proofs: BTreeMap<String, VirtualArchiveWorkProof>,
    },
    /// Exact current M1 activation and result bytes.
    Activated {
        /// Winning or terminal M1 activation receipt.
        receipt: WaitActivationReceipt,
        /// Byte-exact activation result Artifact.
        result: ArtifactRecord,
    },
    /// Exact binding-pinned migrator result and immutable Artifact products.
    Migrated {
        /// Complete verified migration command.
        command: RegionMigrationCommand,
        /// Byte-exact coverage proof.
        coverage_evidence: ArtifactRecord,
        /// Exact target source Artifact records.
        target_source_artifacts: Vec<ArtifactRecord>,
    },
    /// Exact immutable archive publication returned by the pinned compactor.
    Compacted {
        /// Verified provider publication and cumulative index updates.
        archive: VirtualCompactionPublication,
    },
    /// Exact selected cold occurrence values and proofs.
    Rehydrated {
        /// One authenticated value for every selected occurrence.
        occurrences: Vec<VirtualRehydratedOccurrence>,
    },
}

/// One worker-slot claim against a pre-existing exact `ExecutionBinding`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualClaimPersistenceCommand {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Complete claim intent. Durable derives the lease epoch and expiry.
    pub command: VirtualClaimCommand,
}

/// One active-claim lease renewal. Durable derives the new lease fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualLeaseRenewalPersistenceCommand {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Complete renewal intent.
    pub command: VirtualLeaseRenewalCommand,
}

/// One expired-claim recovery plus its exact optional evidence bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRecoveryPersistenceCommand {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Complete recovery intent.
    pub command: VirtualRecoveryCommand,
    /// Exact retry/failure/cancellation evidence bytes.
    pub artifact: ArtifactRecord,
}

/// One future-only Run fairness update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRunWeightPersistenceCommand {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Complete weight update intent.
    pub command: VirtualRunWeightCommand,
}

/// One certificate-owned archive retirement. Durable derives the exact
/// Resource archive release from the current certificate pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveRetirementPersistenceCommand {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Complete content-derived certificate retirement command.
    pub command: VirtualArchiveRetirementCommand,
}

/// Closed semantic M3 persistence command. Checkpoints, deltas, journals,
/// record schemas, raw payload bytes, and caller-authored lease fences are not
/// representable at this boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "operation",
    content = "command",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum VirtualPersistenceOperation {
    /// Establish one scheduler authority.
    Initialize(VirtualInitializationCommand),
    /// Admit one reducer-selected source page.
    Materialize(VirtualMaterializationCommand),
    /// Apply one identified M1 wait activation.
    ActivateWait(VirtualActivationCommand),
    /// Resolve one active occurrence.
    Resolve(VirtualResolutionPersistenceCommand),
    /// Apply one verified region migration.
    MigrateRegion(VirtualMigrationPersistenceCommand),
    /// Publish one immutable cold archive.
    Compact(VirtualCompactionPersistenceCommand),
    /// Rehydrate exact selected occurrence history.
    Rehydrate(VirtualRehydrationPersistenceCommand),
    /// Claim at most one work item.
    Claim(VirtualClaimPersistenceCommand),
    /// Renew one active claim lease.
    RenewLease(VirtualLeaseRenewalPersistenceCommand),
    /// Recover one expired claim.
    Recover(VirtualRecoveryPersistenceCommand),
    /// Change one Run's future fairness weight.
    SetRunWeight(VirtualRunWeightPersistenceCommand),
    /// Atomically retire one certificate and release its archive pin.
    RetireArchive(VirtualArchiveRetirementPersistenceCommand),
}

/// Content-addressed closed command admitted by the Durable coordinator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualPersistenceCommand {
    /// Persistence envelope generation.
    pub persistence_version: String,
    /// Content identity of the complete semantic operation.
    pub persistence_id: String,
    /// Closed semantic action; no persistence lowering values are accepted.
    pub operation: VirtualPersistenceOperation,
}

/// Closed result of one semantic virtual transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "outcome",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum VirtualPersistenceOutcome {
    /// Scheduler authority was established with this many regions.
    Initialized {
        /// Exact number of regions in the initialized bounded current.
        region_count: u64,
    },
    /// One source page made bounded work visible.
    Materialized {
        /// Exact reducer-selected region.
        region_id: String,
        /// Number of newly visible work identities.
        materialized: u64,
    },
    /// One identified M1 activation moved parked work to Ready.
    Activated {
        /// Exact consumed M1 activation identity.
        activation_id: String,
        /// Number of matching parked work items moved to Ready.
        woken: u64,
    },
    /// One active occurrence was resolved.
    Resolved(WorkResolutionReceipt),
    /// One topology migration was committed.
    Migrated(RegionMigrationReceipt),
    /// One completed prefix was moved to immutable cold history.
    Compacted(VirtualCompactionReceipt),
    /// Exact selected cold occurrences were restored hot.
    Rehydrated(VirtualRehydrationReceipt),
    /// One capacity slot observed or claimed work.
    Claimed(VirtualClaimReceipt),
    /// One active claim received a later lease fence.
    LeaseRenewed(VirtualLeaseRenewalReceipt),
    /// One expired active claim was recovered.
    Recovered(VirtualRecoveryReceipt),
    /// One Run's future scheduling share changed.
    RunWeightSet(VirtualRunWeightReceipt),
    /// One cold certificate and its archive pin were retired together.
    ArchiveRetired(VirtualArchiveRetirementReceipt),
}

/// Content roots of every normalized keyed Virtual `StateRoot` family.
///
/// These are witnesses to Durable-owned maps, not caller-selectable storage
/// locations. Durable derives every resulting root from the reducer's typed
/// leaf mutations in the same CAS as the current leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualStateRoots {
    /// Active and retired region leaves keyed by region identity.
    pub regions: String,
    /// Exactly materializable region identities in authenticated map order.
    pub active_regions: String,
    /// Parked work leaves keyed by work identity.
    pub parked: String,
    /// Bounded parked-reason index pages keyed by reason and page identity.
    pub parked_index: String,
    /// Hot exact work identities and latest fences keyed by work identity.
    pub work: String,
    /// Hot occurrence records keyed by occurrence identity.
    pub occurrences: String,
    /// Run weight and deficit leaves keyed by Run identity.
    pub runs: String,
    /// Applied migration receipts keyed by migration identity.
    pub migrations: String,
    /// Active or retired compaction certificates keyed by certificate identity.
    pub certificates: String,
}

/// Hard-bounded scheduler frontier retained in the current leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualFrontierCurrent {
    /// Ready work grouped by Run in deterministic priority order.
    pub ready: BTreeMap<String, VecDeque<WorkItem>>,
    /// Active fenced claims keyed by work identity.
    pub active: BTreeMap<String, ClaimedWork>,
    /// Successful dispatch sequence used by priority aging.
    pub dispatch_sequence: u64,
    /// Dispatch sequence at which every ready work identity became eligible.
    pub ready_since: BTreeMap<String, u64>,
    /// Exact bounded capacity directory for parked M1 Wait reasons.
    ///
    /// A reason is present exactly while at least one hot work item is parked
    /// under that Wait identity. The retained counts and byte charges describe
    /// the complete `ParkedIndex`/`Parked`/`Work` source and mutation set required to
    /// wake that reason. This lets an activation intersect its applied Wait set
    /// without issuing one negative lookup per unrelated M1 target, and makes
    /// every legal aggregate wake representable before the parked state commits.
    pub wait_activations: BTreeMap<String, VirtualWaitActivationCapacity>,
    /// Last Run selected by deterministic weighted fairness.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub last_run: Option<String>,
    /// Last region selected by deterministic source visibility.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub last_region: Option<String>,
}

/// Exact future source and mutation charge for one parked M1 Wait reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualWaitActivationCapacity {
    /// Number of work identities parked under this exact Wait.
    pub work_items: u64,
    /// Number of non-empty `ParkedIndex` pages in its complete chain.
    pub index_pages: u64,
    /// Sum of the exact canonical source-read charges for every index, `Parked`,
    /// and `Work` leaf required by a future activation.
    pub source_bytes: u64,
    /// Sum of the exact canonical bytes of every future `ParkedIndex` deletion,
    /// `Parked` deletion, and `Work`-to-`Ready` mutation.
    pub mutation_bytes: u64,
}

impl VirtualWaitActivationCapacity {
    fn source_items(self) -> ProtocolResult<u64> {
        self.work_items
            .checked_mul(2)
            .and_then(|items| items.checked_add(self.index_pages))
            .filter(|items| *items <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                ProtocolError::Validation(
                    "Virtual Wait activation source-item charge overflowed".to_owned(),
                )
            })
    }

    fn verify(self) -> ProtocolResult<()> {
        validate_positive_exact("Virtual Wait activation work count", self.work_items)?;
        validate_positive_exact("Virtual Wait activation index-page count", self.index_pages)?;
        validate_positive_exact("Virtual Wait activation source bytes", self.source_bytes)?;
        validate_positive_exact(
            "Virtual Wait activation mutation bytes",
            self.mutation_bytes,
        )?;
        let expected_pages = self
            .work_items
            .checked_add(MAX_VIRTUAL_PARKED_INDEX_PAGE_ITEMS as u64 - 1)
            .map(|work| work / MAX_VIRTUAL_PARKED_INDEX_PAGE_ITEMS as u64)
            .ok_or_else(|| {
                ProtocolError::Validation(
                    "Virtual Wait activation page-count calculation overflowed".to_owned(),
                )
            })?;
        if self.work_items > MAX_VIRTUAL_MUTATION_ITEMS as u64 || self.index_pages != expected_pages
        {
            return Err(ProtocolError::IllegalTransition(
                "Virtual Wait activation capacity exceeds one exact wake set or does not match its packed index pages"
                    .to_owned(),
            ));
        }
        let _ = self.source_items()?;
        Ok(())
    }
}

/// Exact cardinalities of normalized keyed Virtual families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCurrentCounts {
    /// Current number of registered region leaves.
    pub regions: u64,
    /// Current number of materializable region-order leaves.
    pub active_regions: u64,
    /// Current number of parked work leaves.
    pub parked: u64,
    /// Current number of hot work-identity leaves.
    pub hot_work: u64,
    /// Current number of hot occurrence leaves.
    pub hot_occurrences: u64,
    /// Current number of Run fairness leaves.
    pub runs: u64,
    /// Current number of migration leaves.
    pub migrations: u64,
    /// Current number of certificate leaves, including retired audit leaves.
    pub certificates: u64,
}

/// Receipt-independent semantic body of one exact bounded current.
///
/// The body deliberately excludes the receipt identity and the `StateRoot` maps
/// that store the current and receipt themselves. Durable can therefore apply
/// the receipt-bound typed mutations, derive these semantic roots, seal this
/// body, and only then seal the receipt and outer current without a hash cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCurrentBody {
    /// Current-body wire generation.
    pub body_version: String,
    /// Content identity of every body field except this identity.
    pub body_id: String,
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Frozen frontier limits established at initialization.
    pub limits: FrontierLimits,
    /// Frozen deterministic fairness policy.
    pub scheduling_policy: SchedulingPolicy,
    /// Immutable archive/index provider generation selected at initialization.
    pub archive: VirtualArchiveBinding,
    /// Hard-bounded ready and active scheduler frontier.
    pub frontier: VirtualFrontierCurrent,
    /// Exact normalized keyed-family roots committed with this current.
    pub roots: VirtualStateRoots,
    /// Cumulative authenticated locator for every work identity moved cold.
    pub archived_work_index_root_digest: String,
    /// Cumulative authenticated locator for every command receipt moved cold.
    pub archived_command_index_root_digest: String,
    /// Exact normalized keyed-family cardinalities.
    pub counts: VirtualCurrentCounts,
}

/// Exact bounded current authority for one virtual scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCurrent {
    /// Current-projection wire generation.
    pub current_version: String,
    /// Content identity of the body and producing receipt.
    pub current_id: String,
    /// Receipt-independent semantic projection and `StateRoot` witnesses.
    pub body: VirtualCurrentBody,
    /// Exact persistence receipt which produced this projection.
    pub last_receipt_id: String,
}

/// Lifecycle of one normalized region leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum VirtualRegionLifecycle {
    /// The region remains eligible for materialization.
    Active,
    /// A verified migration retired the region for future materialization.
    Retired {
        /// Exact migration receipt owning the retirement.
        migration_id: String,
    },
}

/// One region leaf in the normalized Virtual `StateRoot` family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRegionCurrent {
    /// Region-leaf generation.
    pub leaf_version: String,
    /// Owning scheduler namespace.
    pub scheduler_id: String,
    /// Complete exact region authority.
    pub region: VirtualRegion,
    /// Current region lifecycle.
    pub lifecycle: VirtualRegionLifecycle,
    /// Exact number of hot work leaves still owned by this region.
    pub hot_work_count: u64,
    /// Exact number of hot occurrence leaves still owned by this region.
    pub hot_occurrence_count: u64,
    /// Accepted cold certificate for this region, when any.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub compaction_certificate_id: Option<String>,
}

/// One exact materializable-region entry in the authenticated ordering family.
///
/// Retired and exhausted regions remain in `Regions` for audit but never remain
/// in this family, so selecting the next source page is one omission-free map
/// successor lookup independent of accumulated topology history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualActiveRegionCurrent {
    /// Active-region order leaf generation.
    pub leaf_version: String,
    /// Owning scheduler namespace.
    pub scheduler_id: String,
    /// Exact non-retired, non-exhausted region identity.
    pub region_id: String,
}

/// Placement of one hot exact work identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VirtualWorkPlacement {
    /// The work is present in the bounded ready frontier.
    Ready,
    /// The work is present in the bounded active frontier.
    Active,
    /// The work is stored in the keyed parked family.
    Parked,
    /// The latest occurrence is terminal and awaits cold compaction.
    Terminal,
}

/// One hot work identity and its latest exact occurrence fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualWorkCurrent {
    /// Work-leaf generation.
    pub leaf_version: String,
    /// Owning scheduler namespace.
    pub scheduler_id: String,
    /// Complete immutable logical work item.
    pub item: WorkItem,
    /// Latest claim epoch, zero before the first claim.
    pub max_epoch: u64,
    /// Latest exact occurrence identity, absent before the first claim.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub latest_occurrence_id: Option<String>,
    /// Current hot placement.
    pub placement: VirtualWorkPlacement,
}

/// One parked-work leaf keyed by exact work identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualParkedCurrent {
    /// Parked-leaf generation.
    pub leaf_version: String,
    /// Owning scheduler namespace.
    pub scheduler_id: String,
    /// Exact parked item and reason.
    pub parked: ParkedWork,
}

/// One bounded page of the exact parked-reason index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualParkedIndexPage {
    /// Parked-index page generation.
    pub page_version: String,
    /// Owning scheduler namespace.
    pub scheduler_id: String,
    /// Exact indexed reason.
    pub reason: ParkReason,
    /// Zero-based stable page number.
    pub page: u64,
    /// Exact work identities in this bounded page.
    pub work_ids: BTreeSet<String>,
    /// Next page number, absent on the terminal page.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub next_page: Option<u64>,
}

/// One exact occurrence leaf in the normalized Virtual `StateRoot` family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualOccurrenceCurrent {
    /// Occurrence-leaf generation.
    pub leaf_version: String,
    /// Owning scheduler namespace.
    pub scheduler_id: String,
    /// Complete exact occurrence record.
    pub occurrence: WorkOccurrence,
}

/// One Run's deterministic weighted-fairness state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRunCurrent {
    /// Run-leaf generation.
    pub leaf_version: String,
    /// Owning scheduler namespace.
    pub scheduler_id: String,
    /// Exact Run namespace.
    pub run_id: String,
    /// Immutable Plan-selection authority configured at scheduler genesis.
    pub execution: VirtualRunExecution,
    /// Positive future scheduling weight.
    pub weight: u32,
    /// Current exact weighted deficit.
    pub deficit: u64,
}

/// One applied migration leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualMigrationCurrent {
    /// Migration-leaf generation.
    pub leaf_version: String,
    /// Owning scheduler namespace.
    pub scheduler_id: String,
    /// Complete exact migration receipt.
    pub receipt: RegionMigrationReceipt,
}

/// Lifecycle of one immutable compaction certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum VirtualCertificateLifecycle {
    /// Resource retention remains active.
    Active,
    /// Certificate authority and its archive pin were retired together.
    Retired {
        /// Exact retirement receipt owning the Resource release.
        receipt: Box<VirtualArchiveRetirementReceipt>,
    },
}

/// One compaction-certificate leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCertificateCurrent {
    /// Certificate-leaf generation.
    pub leaf_version: String,
    /// Owning scheduler namespace.
    pub scheduler_id: String,
    /// Complete immutable certificate.
    pub certificate: VirtualCompactionCertificate,
    /// Current certificate lifecycle.
    pub lifecycle: VirtualCertificateLifecycle,
}

/// Normalized keyed family changed by one typed Virtual mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VirtualStateFamily {
    /// Region lifecycle and cursor authority.
    Regions,
    /// Materializable region order, excluding retired and exhausted history.
    ActiveRegions,
    /// Parked work by exact work identity.
    Parked,
    /// Bounded parked-reason index pages.
    ParkedIndex,
    /// Hot work identity and latest fence.
    Work,
    /// Hot occurrence records.
    Occurrences,
    /// Run fairness state.
    Runs,
    /// Applied migration receipts.
    Migrations,
    /// Active or retired compaction certificates.
    Certificates,
}

/// One exact typed leaf stored in a normalized Virtual `StateRoot` family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "family",
    content = "leaf",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum VirtualStateLeaf {
    /// Region lifecycle and cursor authority.
    Regions(VirtualRegionCurrent),
    /// Materializable region order entry.
    ActiveRegions(VirtualActiveRegionCurrent),
    /// Parked work by exact work identity.
    Parked(VirtualParkedCurrent),
    /// Bounded parked-reason index page.
    ParkedIndex(VirtualParkedIndexPage),
    /// Hot work identity and latest fence.
    Work(VirtualWorkCurrent),
    /// Hot exact occurrence.
    Occurrences(Box<VirtualOccurrenceCurrent>),
    /// Run fairness state.
    Runs(VirtualRunCurrent),
    /// Applied migration receipt.
    Migrations(VirtualMigrationCurrent),
    /// Active or retired compaction certificate.
    Certificates(Box<VirtualCertificateCurrent>),
}

/// One exact membership or non-membership read from a pinned Virtual family.
///
/// The physical map key is carried explicitly even for absence. This lets the
/// pure preparation phase distinguish a proven non-member from a key that
/// Durable has not read yet, without exposing a caller-authored read set on
/// the persistence wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualStateRead {
    family: VirtualStateFamily,
    storage_key: String,
    leaf: Option<VirtualStateLeaf>,
}

/// One bounded page whose key sequence has already been authenticated against
/// an exact `ActiveRegions` family root.
///
/// This is deliberately a non-Serde reduction capability. Durable constructs
/// it only after the lower authenticated-map range verifier accepts the exact
/// root, cursor, one-entry limit, and terminal boundary. The profile layer
/// retains only provider-independent key evidence; physical proof nodes never
/// cross this boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualActiveRegionPage {
    source_root_id: String,
    source_entries: u64,
    after_storage_key: Option<String>,
    storage_keys: Vec<String>,
    has_more: bool,
}

/// Opaque proof that the next materializable region was selected from at most
/// two authenticated `ActiveRegions` pages.
///
/// The first page starts after the current fairness cursor (or at the map head
/// when no cursor exists). A second page is legal only when the first suffix is
/// authenticated empty, and then starts at the map head. This is the complete
/// wrap-around algorithm; no scan or third read is representable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualActiveRegionSelectionProof {
    current_id: String,
    source_root_id: String,
    source_entries: u64,
    previous_region_id: Option<String>,
    selected_storage_key: Option<String>,
    authenticated_page_count: u8,
}

impl VirtualActiveRegionPage {
    /// Seal one page after Durable has verified the lower authenticated range.
    ///
    /// `storage_keys` contains zero or one exact map key because materialization
    /// selection uses a one-entry page. `has_more` is the verifier-authenticated
    /// successor boundary, not a provider hint.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn from_authenticated_range(
        source_root_id: impl Into<String>,
        source_entries: u64,
        after_storage_key: Option<String>,
        storage_keys: Vec<String>,
        has_more: bool,
    ) -> ProtocolResult<Self> {
        let page = Self {
            source_root_id: source_root_id.into(),
            source_entries,
            after_storage_key,
            storage_keys,
            has_more,
        };
        page.verify()?;
        Ok(page)
    }

    /// Exact semantic family-root identity authenticated by the page.
    pub fn source_root_id(&self) -> &str {
        &self.source_root_id
    }

    /// Exact number of entries committed by the authenticated source root.
    pub const fn source_entries(&self) -> u64 {
        self.source_entries
    }

    /// Exact exclusive cursor supplied to the authenticated range verifier.
    pub fn after_storage_key(&self) -> Option<&str> {
        self.after_storage_key.as_deref()
    }

    /// Zero or one authenticated map key returned by this selection page.
    pub fn storage_keys(&self) -> &[String] {
        &self.storage_keys
    }

    /// Whether the authenticated range contains another entry after this page.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    fn verify(&self) -> ProtocolResult<()> {
        validate_content_id("Virtual active-region family root", &self.source_root_id)?;
        validate_exact("Virtual active-region source count", self.source_entries)?;
        if let Some(after) = &self.after_storage_key {
            validate_content_id("Virtual active-region page cursor", after)?;
        }
        if self.storage_keys.len() > 1 {
            return Err(ProtocolError::Validation(
                "Virtual active-region selection page exceeds its exact one-entry bound".to_owned(),
            ));
        }
        for key in &self.storage_keys {
            validate_content_id("Virtual active-region page key", key)?;
        }
        if self.storage_keys.is_empty() && self.has_more {
            return Err(ProtocolError::Integrity {
                code: "virtual_active_region_page_boundary_mismatch".to_owned(),
                message: "an empty authenticated page cannot retain a successor boundary"
                    .to_owned(),
            });
        }
        let logical_bytes = self
            .source_root_id
            .len()
            .checked_add(self.after_storage_key.as_ref().map_or(0, String::len))
            .and_then(|value| {
                self.storage_keys
                    .iter()
                    .try_fold(value, |total, key| total.checked_add(key.len()))
            })
            .and_then(|value| value.checked_add(std::mem::size_of::<u64>() + 1))
            .ok_or_else(|| {
                ProtocolError::Validation(
                    "Virtual active-region page byte accounting overflowed".to_owned(),
                )
            })?;
        if logical_bytes > MAX_VIRTUAL_ACTIVE_REGION_PAGE_BYTES {
            return Err(ProtocolError::Validation(
                "Virtual active-region page exceeds its hard logical byte bound".to_owned(),
            ));
        }
        Ok(())
    }
}

impl VirtualActiveRegionSelectionProof {
    /// Close the one-read-or-wrap-at-most-once selection algorithm over exact
    /// authenticated pages from the current `ActiveRegions` root.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn from_authenticated_pages(
        current: &VirtualCurrent,
        first: &VirtualActiveRegionPage,
        wrapped: Option<&VirtualActiveRegionPage>,
    ) -> ProtocolResult<Self> {
        current.verify()?;
        first.verify()?;
        let expected_root = &current.body.roots.active_regions;
        let expected_entries = current.body.counts.active_regions;
        if first.source_root_id != *expected_root || first.source_entries != expected_entries {
            return Err(ProtocolError::Integrity {
                code: "virtual_active_region_page_root_mismatch".to_owned(),
                message: "Virtual active-region page does not authenticate the pinned current root"
                    .to_owned(),
            });
        }
        let expected_after = current
            .body
            .frontier
            .last_region
            .as_deref()
            .map(|region_id| virtual_active_region_key(&current.body.scheduler_id, region_id))
            .transpose()?;
        if first.after_storage_key != expected_after {
            return Err(ProtocolError::Integrity {
                code: "virtual_active_region_page_cursor_mismatch".to_owned(),
                message:
                    "Virtual active-region page does not start after the retained fairness cursor"
                        .to_owned(),
            });
        }

        let (selected_storage_key, authenticated_page_count) = if let Some(selected) =
            first.storage_keys.first()
        {
            if wrapped.is_some() {
                return Err(ProtocolError::IllegalTransition(
                    "Virtual active-region selection cannot wrap after finding a successor"
                        .to_owned(),
                ));
            }
            (Some(selected.clone()), 1)
        } else if expected_entries == 0 {
            if wrapped.is_some() || current.body.frontier.last_region.is_some() {
                return Err(ProtocolError::Integrity {
                    code: "virtual_active_region_empty_root_mismatch".to_owned(),
                    message: "an empty ActiveRegions root cannot carry a cursor or wrap page"
                        .to_owned(),
                });
            }
            (None, 1)
        } else {
            if current.body.frontier.last_region.is_none() {
                return Err(ProtocolError::Integrity {
                    code: "virtual_active_region_head_omission".to_owned(),
                    message: "a non-empty ActiveRegions head page omitted its first member"
                        .to_owned(),
                });
            }
            let wrapped = wrapped.ok_or_else(|| ProtocolError::Integrity {
                code: "virtual_active_region_wrap_missing".to_owned(),
                message: "an empty authenticated suffix requires one exact head wrap page"
                    .to_owned(),
            })?;
            wrapped.verify()?;
            if wrapped.source_root_id != *expected_root
                || wrapped.source_entries != expected_entries
                || wrapped.after_storage_key.is_some()
                || wrapped.storage_keys.len() != 1
            {
                return Err(ProtocolError::Integrity {
                        code: "virtual_active_region_wrap_mismatch".to_owned(),
                        message: "Virtual active-region wrap page changed root, count, head cursor, or first member"
                            .to_owned(),
                    });
            }
            (wrapped.storage_keys.first().cloned(), 2)
        };

        let proof = Self {
            current_id: current.current_id.clone(),
            source_root_id: expected_root.clone(),
            source_entries: expected_entries,
            previous_region_id: current.body.frontier.last_region.clone(),
            selected_storage_key,
            authenticated_page_count,
        };
        proof.verify_for(current)?;
        Ok(proof)
    }

    /// Exact selected active-region storage key, absent only for an empty map.
    pub fn selected_storage_key(&self) -> Option<&str> {
        self.selected_storage_key.as_deref()
    }

    /// Number of authenticated pages used by the closed selection algorithm.
    pub const fn authenticated_page_count(&self) -> u8 {
        self.authenticated_page_count
    }

    fn verify_for(&self, current: &VirtualCurrent) -> ProtocolResult<()> {
        current.verify()?;
        if self.current_id != current.current_id
            || self.source_root_id != current.body.roots.active_regions
            || self.source_entries != current.body.counts.active_regions
            || self.previous_region_id != current.body.frontier.last_region
            || !(1..=MAX_VIRTUAL_ACTIVE_REGION_SELECTION_PAGES)
                .contains(&self.authenticated_page_count)
        {
            return Err(ProtocolError::Integrity {
                code: "virtual_active_region_selection_authority_mismatch".to_owned(),
                message: "Virtual active-region selection does not bind the exact pinned current"
                    .to_owned(),
            });
        }
        if (self.source_entries == 0) != self.selected_storage_key.is_none()
            || (self.previous_region_id.is_none() && self.authenticated_page_count != 1)
        {
            return Err(ProtocolError::Integrity {
                code: "virtual_active_region_selection_shape_mismatch".to_owned(),
                message: "Virtual active-region selection has an impossible empty or wrap shape"
                    .to_owned(),
            });
        }
        if let Some(key) = &self.selected_storage_key {
            validate_content_id("Virtual selected active-region key", key)?;
        }
        let logical_bytes = self
            .current_id
            .len()
            .checked_add(self.source_root_id.len())
            .and_then(|value| {
                value.checked_add(self.previous_region_id.as_ref().map_or(0, String::len))
            })
            .and_then(|value| {
                value.checked_add(self.selected_storage_key.as_ref().map_or(0, String::len))
            })
            .and_then(|value| value.checked_add(std::mem::size_of::<u64>() + 1))
            .ok_or_else(|| {
                ProtocolError::Validation(
                    "Virtual active-region selection byte accounting overflowed".to_owned(),
                )
            })?;
        if logical_bytes > MAX_VIRTUAL_ACTIVE_REGION_SELECTION_BYTES {
            return Err(ProtocolError::Validation(
                "Virtual active-region selection exceeds its hard logical byte bound".to_owned(),
            ));
        }
        Ok(())
    }
}

impl VirtualStateRead {
    /// Seal one exact pinned family lookup.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn new(
        family: VirtualStateFamily,
        storage_key: impl Into<String>,
        leaf: Option<VirtualStateLeaf>,
    ) -> ProtocolResult<Self> {
        let read = Self {
            family,
            storage_key: storage_key.into(),
            leaf,
        };
        read.verify()?;
        Ok(read)
    }

    /// Return the normalized family queried by Durable.
    pub const fn family(&self) -> VirtualStateFamily {
        self.family
    }

    /// Return the exact content-addressed map key queried by Durable.
    pub fn storage_key(&self) -> &str {
        &self.storage_key
    }

    /// Return the exact member, or `None` for a proven non-member.
    pub fn leaf(&self) -> Option<&VirtualStateLeaf> {
        self.leaf.as_ref()
    }

    fn verify(&self) -> ProtocolResult<()> {
        validate_content_id("Virtual normalized storage key", &self.storage_key)?;
        if let Some(leaf) = &self.leaf {
            leaf.verify()?;
            if leaf.family() != self.family || leaf.storage_key()? != self.storage_key {
                return Err(ProtocolError::IdentityMismatch(
                    "Virtual pinned read changed its exact family or storage key".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

impl VirtualStateLeaf {
    /// Return the normalized `StateRoot` family that owns this leaf.
    pub const fn family(&self) -> VirtualStateFamily {
        match self {
            Self::Regions(_) => VirtualStateFamily::Regions,
            Self::ActiveRegions(_) => VirtualStateFamily::ActiveRegions,
            Self::Parked(_) => VirtualStateFamily::Parked,
            Self::ParkedIndex(_) => VirtualStateFamily::ParkedIndex,
            Self::Work(_) => VirtualStateFamily::Work,
            Self::Occurrences(_) => VirtualStateFamily::Occurrences,
            Self::Runs(_) => VirtualStateFamily::Runs,
            Self::Migrations(_) => VirtualStateFamily::Migrations,
            Self::Certificates(_) => VirtualStateFamily::Certificates,
        }
    }

    /// Return the scheduler namespace that owns this leaf.
    pub fn scheduler_id(&self) -> &str {
        match self {
            Self::Regions(leaf) => &leaf.scheduler_id,
            Self::ActiveRegions(leaf) => &leaf.scheduler_id,
            Self::Parked(leaf) => &leaf.scheduler_id,
            Self::ParkedIndex(leaf) => &leaf.scheduler_id,
            Self::Work(leaf) => &leaf.scheduler_id,
            Self::Occurrences(leaf) => &leaf.scheduler_id,
            Self::Runs(leaf) => &leaf.scheduler_id,
            Self::Migrations(leaf) => &leaf.scheduler_id,
            Self::Certificates(leaf) => &leaf.scheduler_id,
        }
    }

    /// Verify the complete typed leaf independently of a physical `StateRoot`.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        match self {
            Self::Regions(leaf) => leaf.verify(),
            Self::ActiveRegions(leaf) => leaf.verify(),
            Self::Parked(leaf) => leaf.verify(),
            Self::ParkedIndex(leaf) => leaf.verify(),
            Self::Work(leaf) => leaf.verify(),
            Self::Occurrences(leaf) => leaf.verify(),
            Self::Runs(leaf) => leaf.verify(),
            Self::Migrations(leaf) => leaf.verify(),
            Self::Certificates(leaf) => leaf.verify(),
        }
    }

    /// Derive the unique global `StateRoot` storage key for this typed leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn storage_key(&self) -> ProtocolResult<String> {
        self.verify()?;
        virtual_state_storage_key(self.scheduler_id(), self.family(), &self.local_key()?)
    }

    fn local_key(&self) -> ProtocolResult<String> {
        Ok(match self {
            Self::Regions(leaf) => leaf.region.region_id.clone(),
            Self::ActiveRegions(leaf) => leaf.region_id.clone(),
            Self::Parked(leaf) => leaf.parked.item.work_id.clone(),
            Self::ParkedIndex(leaf) => parked_index_local_key(leaf)?,
            Self::Work(leaf) => leaf.item.work_id.clone(),
            Self::Occurrences(leaf) => leaf.occurrence.occurrence_id.clone(),
            Self::Runs(leaf) => leaf.run_id.clone(),
            Self::Migrations(leaf) => leaf.receipt.plan.migration_id.clone(),
            Self::Certificates(leaf) => leaf.certificate.certificate_id.clone(),
        })
    }
}

/// One exact before/after change to a normalized Virtual `StateRoot` leaf.
///
/// Every variant carries the complete prior value when replacing or deleting
/// a leaf. Durable exact-matches that value against the parent root before it
/// applies the replacement, so a stale or cross-scheduler mutation cannot be
/// accepted merely because it reused the same storage key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum VirtualStateMutation {
    /// Change one region leaf.
    Regions {
        /// Exact parent value, absent only for insertion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        before: Option<VirtualRegionCurrent>,
        /// Exact result value, absent only for deletion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        after: Option<VirtualRegionCurrent>,
    },
    /// Change one materializable-region order entry.
    ActiveRegions {
        /// Exact parent value, absent only for insertion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        before: Option<VirtualActiveRegionCurrent>,
        /// Exact result value, absent only for deletion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        after: Option<VirtualActiveRegionCurrent>,
    },
    /// Change one parked-work leaf.
    Parked {
        /// Exact parent value, absent only for insertion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        before: Option<VirtualParkedCurrent>,
        /// Exact result value, absent only for deletion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        after: Option<VirtualParkedCurrent>,
    },
    /// Change one parked-reason index page.
    ParkedIndex {
        /// Exact parent value, absent only for insertion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        before: Option<VirtualParkedIndexPage>,
        /// Exact result value, absent only for deletion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        after: Option<VirtualParkedIndexPage>,
    },
    /// Change one hot-work leaf.
    Work {
        /// Exact parent value, absent only for insertion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        before: Option<VirtualWorkCurrent>,
        /// Exact result value, absent only for deletion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        after: Option<VirtualWorkCurrent>,
    },
    /// Change one occurrence leaf.
    Occurrences {
        /// Exact parent value, absent only for insertion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        before: Option<Box<VirtualOccurrenceCurrent>>,
        /// Exact result value, absent only for deletion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        after: Option<Box<VirtualOccurrenceCurrent>>,
    },
    /// Change one Run-fairness leaf.
    Runs {
        /// Exact parent value, absent only for insertion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        before: Option<VirtualRunCurrent>,
        /// Exact result value, absent only for deletion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        after: Option<VirtualRunCurrent>,
    },
    /// Change one migration leaf.
    Migrations {
        /// Exact parent value, absent only for insertion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        before: Option<VirtualMigrationCurrent>,
        /// Exact result value, absent only for deletion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        after: Option<VirtualMigrationCurrent>,
    },
    /// Change one compaction-certificate leaf.
    Certificates {
        /// Exact parent value, absent only for insertion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        before: Option<Box<VirtualCertificateCurrent>>,
        /// Exact result value, absent only for deletion.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        after: Option<Box<VirtualCertificateCurrent>>,
    },
}

/// Canonically ordered bounded set of typed Virtual leaf mutations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualMutationSet {
    /// Mutation-set wire generation.
    pub mutation_version: String,
    /// Content identity of the exact ordered operations.
    pub mutation_id: String,
    /// Strictly family-and-key ordered unique leaf changes.
    pub operations: Vec<VirtualStateMutation>,
}

/// Exact replay receipt for one closed virtual persistence command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualPersistenceReceipt {
    /// Receipt wire generation.
    pub receipt_version: String,
    /// Content identity of every receipt field except this identity.
    pub receipt_id: String,
    /// Complete admitted semantic command.
    pub command: VirtualPersistenceCommand,
    /// Exact prior current, absent only for initialization.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_current_id: Option<String>,
    /// Exact provider/M1 product constructed internally by Durable.
    pub evidence: VirtualPersistenceEvidence,
    /// Exact bounded normalized leaf changes derived from the parent current.
    pub mutations: VirtualMutationSet,
    /// Receipt-independent resulting current-body identity.
    pub result_body_id: String,
    /// Closed semantic result returned by the reducer.
    pub outcome: VirtualPersistenceOutcome,
}

/// Exact scalar-current query, optionally pinned to one physical `StateRoot`
/// revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCurrentQuery {
    /// Virtual scheduler authority partition to read.
    pub scheduler_id: String,
    /// Exact physical revision constraint, or null to pin the current head once.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub expected_revision: Option<String>,
}

/// Exact command-receipt query, optionally pinned to one physical `StateRoot`
/// revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualReceiptQuery {
    /// Virtual scheduler authority partition that owns the command.
    pub scheduler_id: String,
    /// Stable semantic command identity.
    pub command_id: String,
    /// Exact physical revision constraint, or null to pin the current head once.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub expected_revision: Option<String>,
}

/// Revision-pinned exact scalar-current read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCurrentRead {
    /// Exact physical `StateRoot` revision observed by this read.
    pub observed_revision: String,
    /// Scalar scheduler current, or null when the exact key is absent.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current: Option<VirtualCurrent>,
}

/// Revision-pinned exact all-ever command receipt read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualReceiptRead {
    /// Exact physical `StateRoot` revision observed by this read.
    pub observed_revision: String,
    /// Stable semantic receipt at the exact scheduler-and-command key, or null.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub receipt: Option<VirtualPersistenceReceipt>,
}

/// Non-persisted physical commit envelope returned by Durable's closed Virtual
/// façade. Physical revisions remain outside the content-addressed receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCommit {
    /// Exact physical `StateRoot` revision observed when returning the receipt.
    pub observed_revision: String,
    /// Result revision for a new commit, or null for exact lost-ack replay.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub committed_revision: Option<String>,
    /// Stable semantic receipt, identical on first commit and exact replay.
    pub receipt: VirtualPersistenceReceipt,
}

/// Closed public result of one Virtual claim. The persisted receipt retains
/// only the selected Plan identity and execution-binding reference; a complete
/// verified Plan is returned only for an actual claim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum VirtualClaimOutcome {
    /// No work was eligible under the exact observed scheduler state.
    NoWork {
        /// Complete normalized persistence receipt.
        receipt: Box<VirtualPersistenceReceipt>,
    },
    /// One work item was claimed with its exact executable semantics.
    Claimed {
        /// Complete normalized persistence receipt.
        receipt: Box<VirtualPersistenceReceipt>,
        /// Non-null exact claim projected from the receipt.
        claim: Box<ClaimedWork>,
        /// Exact verified Plan loaded from the same pinned `StateRoot`.
        plan: Box<cymule_core::SealedPlan>,
    },
}

/// Pure reducer output consumed only by Durable's typed single-CAS lowering.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualPostcondition {
    /// Exact bounded current after the semantic transition.
    pub current: VirtualCurrent,
    /// Exact all-ever command replay receipt.
    pub receipt: VirtualPersistenceReceipt,
    /// Exact immutable Artifact records admitted by this transition.
    pub artifacts: Vec<ArtifactRecord>,
    /// Exact archive pin introduced by compaction, absent otherwise.
    pub archive_pin: Option<ResourcePinReceipt>,
    /// Exact archive-pin release introduced by retirement, absent otherwise.
    pub archive_release: Option<ResourceReleaseReceipt>,
}

/// Receipt-independent reducer result before Durable derives semantic roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualCurrentDraft {
    /// Stable semantic scheduler namespace.
    pub scheduler_id: String,
    /// Frozen scheduler frontier bounds.
    pub limits: FrontierLimits,
    /// Frozen deterministic fairness policy.
    pub scheduling_policy: SchedulingPolicy,
    /// Immutable archive/index provider generation selected at initialization.
    pub archive: VirtualArchiveBinding,
    /// Resulting bounded ready and active frontier.
    pub frontier: VirtualFrontierCurrent,
    /// Resulting cumulative archived-work locator root.
    pub archived_work_index_root_digest: String,
    /// Resulting cumulative archived-command locator root.
    pub archived_command_index_root_digest: String,
    /// Resulting exact normalized family cardinalities.
    pub counts: VirtualCurrentCounts,
}

/// Exact current M1 Run/Plan authority used for one claim admission.
///
/// This is deliberately not serializable and is not a persistence capability.
/// Durable constructs it from the same current `StateRoot` used by the commit.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualExecutionAuthority {
    execution_binding: ArtifactRecord,
    selected: Option<VirtualSelectedExecution>,
}

#[derive(Debug, Clone, PartialEq)]
struct VirtualSelectedExecution {
    run_id: String,
    plan: cymule_core::SealedPlan,
}

/// Deterministic claim choice derived before any Evolution selection or other
/// provider work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualClaimPreview {
    /// Exact ready item selected by the bounded fairness reducer.
    pub item: WorkItem,
    /// Immutable execution selector stored in the selected Run leaf.
    pub execution: VirtualRunExecution,
}

/// Failures produced while preparing one exact Virtual reduction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VirtualPreparationError {
    /// A semantic command, source value, or provider product is invalid.
    Protocol(ProtocolError),
    /// One exact family membership or non-membership proof is still required.
    ReadRequired {
        /// Normalized persistent-map family.
        family: VirtualStateFamily,
        /// Exact content-addressed persistent-map key.
        storage_key: String,
    },
}

impl Display for VirtualPreparationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol(error) => Display::fmt(error, formatter),
            Self::ReadRequired {
                family,
                storage_key,
            } => write!(formatter, "read_required: {family:?} {storage_key}"),
        }
    }
}

impl std::error::Error for VirtualPreparationError {}

impl From<ProtocolError> for VirtualPreparationError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

/// Result type for exact Virtual reduction preparation.
pub type VirtualPreparationResult<T> = Result<T, VirtualPreparationError>;

impl VirtualClaimPreview {
    /// Derive the next occurrence identity after Durable exact-loads the
    /// selected work leaf from the same pinned `StateRoot` revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn occurrence_id(&self, work: &VirtualWorkCurrent) -> ProtocolResult<String> {
        work.verify()?;
        if work.item != self.item || work.placement != VirtualWorkPlacement::Ready {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual claim preview changed its exact ready work leaf".to_owned(),
            ));
        }
        let epoch = checked_exact_add("Virtual work claim epoch", work.max_epoch, 1)?;
        cymule_core::content_id(
            VIRTUAL_WORK_OCCURRENCE_VERSION,
            &(work.item.work_id.as_str(), epoch),
        )
        .map_err(ProtocolError::from)
    }
}

/// Exact typed Virtual leaves loaded from the parent `StateRoot`.
///
/// The reducer requires each command's precise key set and rejects both
/// missing and orphan leaves. This value is non-serializable and never enters
/// the durable command wire.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualKeyedSource {
    scheduler_id: String,
    current: Option<VirtualCurrent>,
    lookups: BTreeSet<(VirtualStateFamily, String)>,
    source_bytes: usize,
    regions: BTreeMap<String, VirtualRegionCurrent>,
    active_regions: BTreeMap<String, VirtualActiveRegionCurrent>,
    parked: BTreeMap<String, VirtualParkedCurrent>,
    parked_index: BTreeMap<String, VirtualParkedIndexPage>,
    work: BTreeMap<String, VirtualWorkCurrent>,
    occurrences: BTreeMap<String, VirtualOccurrenceCurrent>,
    runs: BTreeMap<String, VirtualRunCurrent>,
    migrations: BTreeMap<String, VirtualMigrationCurrent>,
    certificates: BTreeMap<String, VirtualCertificateCurrent>,
}

/// Binding-pinned source adapter invoked only by Durable after exact replay
/// lookup and current-state preparation.
pub trait VirtualRegionSourceProvider {
    /// Return the immutable generation implemented by this adapter.
    fn source_binding(&self) -> RegionSourceBinding;

    /// Materialize one deterministic bounded page at the exact current cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned source cannot produce the exact bounded page.
    fn materialize(
        &mut self,
        region: &VirtualRegion,
        limit: usize,
    ) -> ProtocolResult<MaterializedPage>;
}

/// Complete immutable product of one binding-pinned region migration adapter.
///
/// This non-serializable provider result carries every new Artifact referenced
/// by the plan. It is not a persistence capability: Durable verifies the pinned
/// adapter's coverage decision and commits the complete reducer postcondition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualRegionMigrationProposal {
    /// Exact opaque-cursor plan produced by the selected adapter generation.
    pub plan: RegionMigrationPlan,
    /// Complete immutable bytes of the plan's coverage evidence.
    pub coverage_evidence: ArtifactRecord,
    /// Exactly one complete record for each distinct target source reference.
    pub target_source_artifacts: Vec<ArtifactRecord>,
}

impl VirtualRegionMigrationProposal {
    /// Verify exact request correspondence, immutable products, and the shared
    /// aggregate Artifact byte bound before the adapter's coverage verification.
    ///
    /// # Errors
    ///
    /// Returns an error for a different request, missing, duplicate, or extra
    /// material, mismatched Artifact bytes, or an oversized aggregate product.
    pub fn verify_for(
        &self,
        persistence: &VirtualMigrationPersistenceCommand,
    ) -> ProtocolResult<()> {
        let command = RegionMigrationCommand {
            control_version: VIRTUAL_REGION_MIGRATION_CONTROL_VERSION.to_owned(),
            command_id: persistence.command_id.clone(),
            plan: self.plan.clone(),
        };
        verify_migration_evidence(
            persistence,
            &command,
            &self.coverage_evidence,
            &self.target_source_artifacts,
        )
    }

    /// Derive the closed reducer authority after Durable verifies this proposal
    /// with its exact pinned adapter. No Artifact is independently registered.
    ///
    /// # Errors
    ///
    /// Returns the same exact-product validation failures as [`Self::verify_for`].
    pub fn into_authority(
        self,
        persistence: &VirtualMigrationPersistenceCommand,
    ) -> ProtocolResult<VirtualOperationAuthority> {
        let command = RegionMigrationCommand {
            control_version: VIRTUAL_REGION_MIGRATION_CONTROL_VERSION.to_owned(),
            command_id: persistence.command_id.clone(),
            plan: self.plan,
        };
        verify_migration_evidence(
            persistence,
            &command,
            &self.coverage_evidence,
            &self.target_source_artifacts,
        )?;
        Ok(VirtualOperationAuthority::MigrateRegion {
            command,
            coverage_evidence: self.coverage_evidence,
            target_source_artifacts: self.target_source_artifacts,
        })
    }
}

/// Binding-pinned migration adapter invoked only by Durable after exact
/// source-region preparation.
pub trait VirtualRegionMigratorProvider {
    /// Immutable semantic adapter binding.
    fn binding(&self) -> &str;

    /// Immutable implementation revision within the binding.
    fn revision(&self) -> &str;

    /// Produce a replacement plan and all referenced immutable Artifact bytes
    /// from the exact current source leaves.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned generation cannot produce a complete
    /// bounded plan, coverage record, and exact target-source record set.
    fn plan(
        &mut self,
        request: &RegionMigrationRequest,
        sources: &[VirtualRegion],
    ) -> ProtocolResult<VirtualRegionMigrationProposal>;

    /// Verify the complete coverage product under this exact generation.
    ///
    /// # Errors
    ///
    /// Returns an error when the coverage product is incomplete or belongs to another generation.
    fn verify(&mut self, plan: &RegionMigrationPlan) -> ProtocolResult<()>;
}

/// Binding-pinned immutable archive and cumulative-index adapter.
pub trait VirtualArchiveProvider {
    /// Return the immutable generation implemented by this adapter.
    fn archive_binding(&self) -> VirtualArchiveBinding;

    /// Return an exact archived-work membership or absence proof.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact cumulative root or requested proof cannot be verified.
    fn work_index_proof(
        &mut self,
        root_digest: &str,
        work_id: &str,
    ) -> ProtocolResult<VirtualArchiveWorkProof>;

    /// Persist one verified immutable archived-work insertion.
    ///
    /// # Errors
    ///
    /// Returns an error when the insertion conflicts or its immutable readback fails.
    fn insert_work_index(
        &mut self,
        parent_root_digest: &str,
        value: &ArchivedWorkIndex,
    ) -> ProtocolResult<VirtualArchiveWorkIndexUpdate>;

    /// Return an exact archived-command locator membership or absence proof.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact cumulative root or requested proof cannot be verified.
    fn command_index_proof(
        &mut self,
        root_digest: &str,
        journal_id: &str,
        command_id: &str,
    ) -> ProtocolResult<VirtualArchiveCommandIndexProof>;

    /// Persist one verified immutable archived-command locator insertion.
    ///
    /// # Errors
    ///
    /// Returns an error when the insertion conflicts or its immutable readback fails.
    fn insert_command_index(
        &mut self,
        parent_root_digest: &str,
        value: &ArchivedCommandIndex,
    ) -> ProtocolResult<VirtualArchiveCommandIndexUpdate>;

    /// Idempotently publish exact framework-derived manifest bytes and return
    /// their verified immutable Resource publication.
    ///
    /// # Errors
    ///
    /// Returns an error when publication or exact immutable readback fails.
    fn publish_archive(
        &mut self,
        manifest: &VirtualArchiveManifest,
    ) -> ProtocolResult<ResourcePublication>;

    /// Read one exact archived occurrence and its certificate-bound proof.
    ///
    /// # Errors
    ///
    /// Returns an error when the occurrence is absent or its archive proof fails.
    fn rehydrate_occurrence(
        &mut self,
        descriptor: &ResourceHandle,
        occurrence_id: &str,
    ) -> ProtocolResult<VirtualRehydratedOccurrence>;

    /// Read one exact typed historical receipt and its certificate-bound proof.
    ///
    /// # Errors
    ///
    /// Returns an error when the receipt is absent or its archive proof fails.
    fn archived_command(
        &mut self,
        descriptor: &ResourceHandle,
        journal_id: &str,
        command_id: &str,
    ) -> ProtocolResult<VirtualArchivedCommand>;
}

/// Exact provider registry borrowed by Durable's closed Virtual control.
/// Implementations resolve only the full immutable selector supplied by the
/// semantic current or command; mutable defaults and fallback generations are
/// outside this contract.
pub trait VirtualProviders {
    /// Resolve one exact region-source generation.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact immutable provider generation is unavailable.
    fn region_source(
        &mut self,
        binding: &RegionSourceBinding,
    ) -> ProtocolResult<&mut dyn VirtualRegionSourceProvider>;

    /// Resolve one exact region-migrator generation.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact immutable provider generation is unavailable.
    fn region_migrator(
        &mut self,
        binding: &str,
        revision: &str,
    ) -> ProtocolResult<&mut dyn VirtualRegionMigratorProvider>;

    /// Resolve the scheduler's exact archive/index generation.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact immutable provider generation is unavailable.
    fn archive(
        &mut self,
        binding: &VirtualArchiveBinding,
    ) -> ProtocolResult<&mut dyn VirtualArchiveProvider>;
}

/// Empty registry for operations that require no provider.
#[derive(Debug, Default)]
pub struct NoVirtualProviders;

impl VirtualProviders for NoVirtualProviders {
    fn region_source(
        &mut self,
        binding: &RegionSourceBinding,
    ) -> ProtocolResult<&mut dyn VirtualRegionSourceProvider> {
        Err(ProtocolError::Validation(format!(
            "Virtual region source {}@{} is not registered",
            binding.binding, binding.revision
        )))
    }

    fn region_migrator(
        &mut self,
        binding: &str,
        revision: &str,
    ) -> ProtocolResult<&mut dyn VirtualRegionMigratorProvider> {
        Err(ProtocolError::Validation(format!(
            "Virtual region migrator {binding}@{revision} is not registered"
        )))
    }

    fn archive(
        &mut self,
        binding: &VirtualArchiveBinding,
    ) -> ProtocolResult<&mut dyn VirtualArchiveProvider> {
        Err(ProtocolError::Validation(format!(
            "Virtual archive {}@{} is not registered",
            binding.binding, binding.revision
        )))
    }
}

/// Operation-specific exact M1, Resource, and higher-profile evidence.
///
/// No variant is serializable or independently authorizes a write. Durable's
/// closed `commit_virtual` path constructs the matching variant only after it
/// resolves current authority and invokes any binding-pinned provider.
#[derive(Debug, Clone, PartialEq)]
pub enum VirtualOperationAuthority {
    /// Scheduler genesis has no prior cross-profile evidence.
    Initialize,
    /// Exact provider page and archived-work proofs.
    Materialize {
        /// Exact one-page-or-single-wrap authenticated region selection.
        selection: VirtualActiveRegionSelectionProof,
        /// Bounded page returned by the selected `RegionSource`.
        page: MaterializedPage,
        /// Exact cumulative absence proof per returned work identity.
        archived_work_proofs: BTreeMap<String, VirtualArchiveWorkProof>,
    },
    /// Exact winning M1 activation receipt.
    ActivateWait {
        /// Current M1 activation result.
        receipt: WaitActivationReceipt,
        /// Byte-exact M1 activation result Artifact.
        result: ArtifactRecord,
    },
    /// Exact Clock observation for a normal resolution.
    Resolve {
        /// Current-head Clock observation.
        clock: ClockObservation,
    },
    /// Exact binding-pinned migrator result.
    MigrateRegion {
        /// Complete verified migration command.
        command: RegionMigrationCommand,
        /// Byte-exact coverage proof.
        coverage_evidence: ArtifactRecord,
        /// Exact target-region source Artifact records.
        target_source_artifacts: Vec<ArtifactRecord>,
    },
    /// Exact Resource archive pin result derived in this same CAS.
    Compact {
        /// Exact typed cold payload derived from selected keyed leaves and receipts.
        manifest: VirtualArchiveManifest,
        /// Exact immutable provider publication and cumulative index updates.
        archive: VirtualCompactionPublication,
        /// Resulting Resource profile-pin receipt.
        archive_pin: ResourcePinReceipt,
    },
    /// Exact selected cold occurrence values and proofs.
    Rehydrate {
        /// One authenticated value for every selected occurrence.
        occurrences: Vec<VirtualRehydratedOccurrence>,
    },
    /// Current M1 admission and derived slot lease for one claim.
    Claim {
        /// Current-head Clock observation.
        clock: ClockObservation,
        /// Exact lease derived by Durable from current M1 lease authority.
        lease: VirtualClaimLease,
        /// Exact pre-existing binding and optional selected Run/Plan authority.
        /// Empty claims still carry and verify the binding record.
        execution: VirtualExecutionAuthority,
        /// Exact standard Evolution selection result committed by the same
        /// CAS, absent for standalone or empty M3 claims.
        evolution_selection: Option<VirtualEvolutionSelectionLink>,
    },
    /// Current M1 Clock observation and derived replacement lease.
    RenewLease {
        /// Current-head Clock observation.
        clock: ClockObservation,
        /// Exact replacement lease derived by Durable.
        lease: VirtualClaimLease,
    },
    /// Current M1 Clock observation proving lease expiry.
    Recover {
        /// Current-head Clock observation.
        clock: ClockObservation,
    },
    /// Future-only Run weight mutation has no cross-profile evidence.
    SetRunWeight,
    /// Exact Resource archive release derived from the current certificate pin.
    RetireArchive {
        /// Closed release delta applied by Durable.
        release: ResourceArchiveRelease,
        /// Resulting exact Resource release receipt.
        receipt: ResourceReleaseReceipt,
    },
}

/// Complete non-serializable exact authority consumed by the pure reducer.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualReductionAuthority {
    source: VirtualKeyedSource,
    operation: VirtualOperationAuthority,
}

/// Pure semantic result before Durable applies typed leaf mutations.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualReduction {
    /// Complete semantic command reduced against the exact parent.
    pub command: VirtualPersistenceCommand,
    /// Exact parent current identity, absent only for genesis.
    pub parent_current_id: Option<String>,
    /// Exact provider/M1 product retained for audit and replay.
    pub evidence: VirtualPersistenceEvidence,
    /// Receipt-independent current data whose roots Durable must derive.
    pub current: VirtualCurrentDraft,
    /// Exact bounded normalized leaf changes.
    pub mutations: VirtualMutationSet,
    /// Closed semantic result.
    pub outcome: VirtualPersistenceOutcome,
    /// Exact immutable Artifact records admitted in the same CAS.
    pub artifacts: Vec<ArtifactRecord>,
    /// Exact archive pin introduced by compaction.
    pub archive_pin: Option<ResourcePinReceipt>,
    /// Exact archive release introduced by retirement.
    pub archive_release: Option<ResourceReleaseReceipt>,
}

impl VirtualExecutionAuthority {
    /// Construct exact selected Run/Plan/binding evidence loaded by Durable.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn new(
        run_id: impl Into<String>,
        plan: cymule_core::SealedPlan,
        execution_binding: ArtifactRecord,
    ) -> ProtocolResult<Self> {
        let authority = Self {
            execution_binding,
            selected: Some(VirtualSelectedExecution {
                run_id: run_id.into(),
                plan,
            }),
        };
        authority.verify()?;
        Ok(authority)
    }

    /// Construct exact binding-only evidence for an empty claim preview.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn binding_only(execution_binding: ArtifactRecord) -> ProtocolResult<Self> {
        let authority = Self {
            execution_binding,
            selected: None,
        };
        authority.verify()?;
        Ok(authority)
    }

    fn verify(&self) -> ProtocolResult<()> {
        verify_exact_artifact_record(
            &self.execution_binding,
            &self.execution_binding.reference,
            "Virtual execution authority",
        )?;
        validate_execution_binding(&self.execution_binding.reference)?;
        let binding: cymule_runtime::ExecutionBinding =
            serde_json::from_slice(&self.execution_binding.bytes).map_err(|error| {
                ProtocolError::Validation(format!(
                    "Virtual execution authority is not an ExecutionBinding: {error}"
                ))
            })?;
        binding.verify().map_err(|error| {
            ProtocolError::Validation(format!("Virtual execution authority is invalid: {error}"))
        })?;
        if let Some(selected) = &self.selected {
            validate_identity("Virtual execution Run", &selected.run_id)?;
            selected.plan.verify().map_err(ProtocolError::from)?;
            binding.admit_plan(&selected.plan).map_err(|error| {
                ProtocolError::Validation(format!(
                    "Virtual execution authority does not admit the selected Plan: {error}"
                ))
            })?;
        }
        if binding.canonical_bytes().map_err(|error| {
            ProtocolError::Validation(format!(
                "Virtual execution authority cannot be canonically encoded: {error}"
            ))
        })? != self.execution_binding.bytes
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual execution authority bytes are not canonical".to_owned(),
            ));
        }
        Ok(())
    }
}

struct VirtualSourceMembers {
    lookups: BTreeSet<(VirtualStateFamily, String)>,
    source_bytes: usize,
    regions: Vec<VirtualRegionCurrent>,
    active_regions: Vec<VirtualActiveRegionCurrent>,
    parked: Vec<VirtualParkedCurrent>,
    parked_index: Vec<VirtualParkedIndexPage>,
    work: Vec<VirtualWorkCurrent>,
    occurrences: Vec<VirtualOccurrenceCurrent>,
    runs: Vec<VirtualRunCurrent>,
    migrations: Vec<VirtualMigrationCurrent>,
    certificates: Vec<VirtualCertificateCurrent>,
}

fn collect_virtual_source_reads(
    scheduler_id: &str,
    current: Option<&VirtualCurrent>,
    reads: Vec<VirtualStateRead>,
) -> ProtocolResult<VirtualSourceMembers> {
    let mut members = VirtualSourceMembers {
        lookups: BTreeSet::new(),
        source_bytes: current
            .map(cymule_core::canonical_bytes)
            .transpose()?
            .map_or(0, |bytes| bytes.len()),
        regions: Vec::new(),
        active_regions: Vec::new(),
        parked: Vec::new(),
        parked_index: Vec::new(),
        work: Vec::new(),
        occurrences: Vec::new(),
        runs: Vec::new(),
        migrations: Vec::new(),
        certificates: Vec::new(),
    };
    for read in reads {
        read.verify()?;
        if !members
            .lookups
            .insert((read.family, read.storage_key.clone()))
        {
            return Err(ProtocolError::IllegalTransition(
                "Virtual keyed source repeats a family-and-key read".to_owned(),
            ));
        }
        let read_bytes = cymule_core::canonical_bytes(&(
            read.family,
            read.storage_key.as_str(),
            read.leaf.as_ref(),
        ))?
        .len();
        members.source_bytes = members
            .source_bytes
            .checked_add(read_bytes)
            .ok_or_else(|| {
                ProtocolError::Validation(
                    "Virtual keyed source canonical bytes overflowed".to_owned(),
                )
            })?;
        if members.source_bytes > MAX_VIRTUAL_REDUCTION_SOURCE_BYTES {
            return Err(ProtocolError::Validation(
                "Virtual keyed source exceeds its hard aggregate canonical byte bound".to_owned(),
            ));
        }
        let Some(leaf) = read.leaf else {
            continue;
        };
        if leaf.scheduler_id() != scheduler_id {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual keyed source contains a cross-scheduler leaf".to_owned(),
            ));
        }
        match leaf {
            VirtualStateLeaf::Regions(leaf) => members.regions.push(leaf),
            VirtualStateLeaf::ActiveRegions(leaf) => members.active_regions.push(leaf),
            VirtualStateLeaf::Parked(leaf) => members.parked.push(leaf),
            VirtualStateLeaf::ParkedIndex(leaf) => members.parked_index.push(leaf),
            VirtualStateLeaf::Work(leaf) => members.work.push(leaf),
            VirtualStateLeaf::Occurrences(leaf) => members.occurrences.push(*leaf),
            VirtualStateLeaf::Runs(leaf) => members.runs.push(leaf),
            VirtualStateLeaf::Migrations(leaf) => members.migrations.push(leaf),
            VirtualStateLeaf::Certificates(leaf) => members.certificates.push(*leaf),
        }
    }
    Ok(members)
}

impl VirtualKeyedSource {
    /// Construct an exact bounded source from pinned family reads.
    ///
    /// Durable uses this entry point after resolving the command's exact keys
    /// from the same pinned `StateRoot` revision. Both membership and absence
    /// are explicit. Family/key duplicates and leaves belonging to another
    /// scheduler fail before reduction.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn from_reads(
        scheduler_id: impl Into<String>,
        current: Option<VirtualCurrent>,
        reads: Vec<VirtualStateRead>,
    ) -> ProtocolResult<Self> {
        let scheduler_id = scheduler_id.into();
        validate_identity("Virtual scheduler", &scheduler_id)?;
        if let Some(current) = &current {
            current.verify()?;
            if current.body.scheduler_id != scheduler_id {
                return Err(ProtocolError::IdentityMismatch(
                    "Virtual keyed source current belongs to another scheduler".to_owned(),
                ));
            }
        }
        let members = collect_virtual_source_reads(&scheduler_id, current.as_ref(), reads)?;

        let source = Self {
            scheduler_id,
            current,
            lookups: members.lookups,
            source_bytes: members.source_bytes,
            regions: collect_keyed_leaves(members.regions, VirtualRegionCurrent::verify, |leaf| {
                Ok(leaf.region.region_id.clone())
            })?,
            active_regions: collect_keyed_leaves(
                members.active_regions,
                VirtualActiveRegionCurrent::verify,
                |leaf| Ok(leaf.region_id.clone()),
            )?,
            parked: collect_keyed_leaves(members.parked, VirtualParkedCurrent::verify, |leaf| {
                Ok(leaf.parked.item.work_id.clone())
            })?,
            parked_index: collect_keyed_leaves(
                members.parked_index,
                VirtualParkedIndexPage::verify,
                parked_index_local_key,
            )?,
            work: collect_keyed_leaves(members.work, VirtualWorkCurrent::verify, |leaf| {
                Ok(leaf.item.work_id.clone())
            })?,
            occurrences: collect_keyed_leaves(
                members.occurrences,
                VirtualOccurrenceCurrent::verify,
                |leaf| Ok(leaf.occurrence.occurrence_id.clone()),
            )?,
            runs: collect_keyed_leaves(members.runs, VirtualRunCurrent::verify, |leaf| {
                Ok(leaf.run_id.clone())
            })?,
            migrations: collect_keyed_leaves(
                members.migrations,
                VirtualMigrationCurrent::verify,
                |leaf| Ok(leaf.receipt.plan.migration_id.clone()),
            )?,
            certificates: collect_keyed_leaves(
                members.certificates,
                VirtualCertificateCurrent::verify,
                |leaf| Ok(leaf.certificate.certificate_id.clone()),
            )?,
        };
        source.verify_bounds()?;
        Ok(source)
    }

    fn verify_bounds(&self) -> ProtocolResult<()> {
        let members = self
            .regions
            .len()
            .checked_add(self.active_regions.len())
            .and_then(|value| value.checked_add(self.parked.len()))
            .and_then(|value| value.checked_add(self.parked_index.len()))
            .and_then(|value| value.checked_add(self.work.len()))
            .and_then(|value| value.checked_add(self.occurrences.len()))
            .and_then(|value| value.checked_add(self.runs.len()))
            .and_then(|value| value.checked_add(self.migrations.len()))
            .and_then(|value| value.checked_add(self.certificates.len()))
            .ok_or_else(|| {
                ProtocolError::Validation("Virtual keyed source count overflowed".to_owned())
            })?;
        if members > self.lookups.len() || self.lookups.len() > MAX_VIRTUAL_REDUCTION_SOURCE_ITEMS {
            return Err(ProtocolError::Validation(
                "Virtual reducer loaded more exact source leaves than one bounded transition permits"
                    .to_owned(),
            ));
        }
        let scheduler = self
            .current
            .as_ref()
            .map_or(self.scheduler_id.as_str(), |current| {
                current.body.scheduler_id.as_str()
            });
        for leaf_scheduler in self
            .regions
            .values()
            .map(|leaf| leaf.scheduler_id.as_str())
            .chain(
                self.active_regions
                    .values()
                    .map(|leaf| leaf.scheduler_id.as_str()),
            )
            .chain(self.parked.values().map(|leaf| leaf.scheduler_id.as_str()))
            .chain(
                self.parked_index
                    .values()
                    .map(|leaf| leaf.scheduler_id.as_str()),
            )
            .chain(self.work.values().map(|leaf| leaf.scheduler_id.as_str()))
            .chain(
                self.occurrences
                    .values()
                    .map(|leaf| leaf.scheduler_id.as_str()),
            )
            .chain(self.runs.values().map(|leaf| leaf.scheduler_id.as_str()))
            .chain(
                self.migrations
                    .values()
                    .map(|leaf| leaf.scheduler_id.as_str()),
            )
            .chain(
                self.certificates
                    .values()
                    .map(|leaf| leaf.scheduler_id.as_str()),
            )
        {
            if scheduler != leaf_scheduler {
                return Err(ProtocolError::IdentityMismatch(
                    "Virtual keyed source contains a cross-scheduler leaf".to_owned(),
                ));
            }
        }
        if self.source_bytes > MAX_VIRTUAL_REDUCTION_SOURCE_BYTES {
            return Err(ProtocolError::Validation(
                "Virtual keyed source exceeds its hard aggregate canonical byte bound".to_owned(),
            ));
        }
        Ok(())
    }

    fn require_read(
        &self,
        family: VirtualStateFamily,
        storage_key: String,
    ) -> VirtualPreparationResult<()> {
        if self.lookups.contains(&(family, storage_key.clone())) {
            Ok(())
        } else {
            Err(VirtualPreparationError::ReadRequired {
                family,
                storage_key,
            })
        }
    }
}

impl VirtualReductionAuthority {
    /// Pair exact keyed parent leaves with operation-specific current evidence.
    pub fn new(source: VirtualKeyedSource, operation: VirtualOperationAuthority) -> Self {
        Self { source, operation }
    }
}

impl VirtualReduction {
    /// Seal Durable-derived semantic roots into the exact receipt and current.
    ///
    /// This helper is pure. The Durable façade must first apply `mutations`
    /// against the exact parent `StateRoot`, derive `roots`, and exact-match the
    /// returned postcondition before its one CAS.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn finish(self, roots: VirtualStateRoots) -> ProtocolResult<VirtualPostcondition> {
        let body = VirtualCurrentBody::new(self.current, roots)?;
        let receipt = VirtualPersistenceReceipt::new(
            self.command,
            self.parent_current_id,
            self.evidence,
            self.mutations,
            body.body_id.clone(),
            self.outcome,
        )?;
        let current = VirtualCurrent::new(body, receipt.receipt_id.clone())?;
        let postcondition = VirtualPostcondition {
            current,
            receipt,
            artifacts: self.artifacts,
            archive_pin: self.archive_pin,
            archive_release: self.archive_release,
        };
        postcondition.verify()?;
        Ok(postcondition)
    }
}

/// Complete deterministic exact-read preflight for one provider-bound action.
///
/// Durable must satisfy this function from one pinned `StateRoot` before it
/// invokes a region source, migrator, or archive provider. Materialization also
/// supplies the typed authenticated successor selection from that same root;
/// every other provider operation supplies `None`. Provider-returned identities
/// are checked later by [`prepare_virtual`], which may request additional exact
/// reads for those dynamic keys before producing a postcondition.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn preflight_virtual_provider(
    command: &VirtualPersistenceCommand,
    source: &VirtualKeyedSource,
    active_region_selection: Option<&VirtualActiveRegionSelectionProof>,
) -> VirtualPreparationResult<()> {
    command.verify()?;
    source.verify_bounds()?;
    if source.scheduler_id != command.scheduler_id() {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual provider preflight targets a different pinned scheduler".to_owned(),
        )
        .into());
    }
    let current = require_virtual_current(source)?;
    let mut required = BTreeSet::new();
    match &command.operation {
        VirtualPersistenceOperation::Materialize(operation) => {
            preflight_virtual_materialization(
                command,
                operation,
                source,
                current,
                active_region_selection,
                &mut required,
            )?;
        }
        VirtualPersistenceOperation::MigrateRegion(operation) => {
            reject_active_selection(active_region_selection, "migration")?;
            preflight_virtual_migration(operation, source, &mut required)?;
        }
        VirtualPersistenceOperation::Compact(operation) => {
            reject_active_selection(active_region_selection, "compaction")?;
            preflight_virtual_compaction(operation, source, &mut required)?;
        }
        VirtualPersistenceOperation::Rehydrate(operation) => {
            reject_active_selection(active_region_selection, "rehydration")?;
            preflight_virtual_rehydration(operation, source, &mut required)?;
        }
        _ => {
            return Err(ProtocolError::Validation(
                "Virtual provider preflight accepts only provider-bound semantic operations"
                    .to_owned(),
            )
            .into());
        }
    }
    if source.lookups != required {
        return Err(ProtocolError::IllegalTransition(
            "Virtual provider preflight source contains an orphan exact family read".to_owned(),
        )
        .into());
    }
    Ok(())
}

/// Derive the exact certificate needed to key cumulative archived-command
/// locator insertions before the final Virtual reduction.
///
/// Durable calls this after provider preflight, manifest publication, and all
/// archived-work index insertions. `archive.command_index_updates` must still
/// be empty. The returned certificate is pure authority only: Durable uses its
/// ID to build the ordered command-index updates, derives the Resource archive
/// pin, and then supplies the complete publication to [`prepare_virtual`].
///
/// # Errors
///
/// Returns an error when the manifest, publication, roots, work-index chain,
/// or selected hot closure does not verify exactly against the preflighted
/// pinned source.
pub fn prepare_virtual_compaction_certificate(
    command: &VirtualCompactionPersistenceCommand,
    source: &VirtualKeyedSource,
    manifest: &VirtualArchiveManifest,
    archive: &VirtualCompactionPublication,
) -> ProtocolResult<VirtualCompactionCertificate> {
    if !archive.command_index_updates.is_empty() {
        return Err(ProtocolError::IllegalTransition(
            "Virtual staged compaction certificate precedes command-index insertions".to_owned(),
        ));
    }
    let (current, _) = validate_virtual_compaction_selection(command, source, manifest, archive)?;
    let (occurrence_root, command_root) =
        validate_virtual_compaction_publication(manifest, archive)?;
    build_virtual_compaction_certificate(
        &command.command,
        current,
        manifest,
        archive,
        occurrence_root,
        command_root,
    )
}

fn reject_active_selection(
    selection: Option<&VirtualActiveRegionSelectionProof>,
    operation: &str,
) -> VirtualPreparationResult<()> {
    if selection.is_some() {
        return Err(ProtocolError::IllegalTransition(format!(
            "Virtual {operation} provider preflight received active-region selection"
        ))
        .into());
    }
    Ok(())
}

fn preflight_virtual_materialization(
    command: &VirtualPersistenceCommand,
    operation: &VirtualMaterializationCommand,
    source: &VirtualKeyedSource,
    current: &VirtualCurrent,
    selection: Option<&VirtualActiveRegionSelectionProof>,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    let selection = selection.ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Virtual materialization provider preflight requires authenticated active-region selection"
                .to_owned(),
        )
    })?;
    selection.verify_for(current)?;
    let expected_key = virtual_active_region_key(command.scheduler_id(), &operation.region_id)?;
    if selection.selected_storage_key() != Some(expected_key.as_str()) {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual materialization provider preflight changed the authenticated region selection"
                .to_owned(),
        )
        .into());
    }
    for family in [
        VirtualStateFamily::Regions,
        VirtualStateFamily::ActiveRegions,
    ] {
        require_virtual_local_read(source, required, family, &operation.region_id)?;
    }
    if source.regions.keys().collect::<BTreeSet<_>>() != BTreeSet::from([&operation.region_id])
        || source.active_regions.keys().collect::<BTreeSet<_>>()
            != BTreeSet::from([&operation.region_id])
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual materialization provider preflight requires its exact active region"
                .to_owned(),
        )
        .into());
    }
    Ok(())
}

fn preflight_virtual_migration(
    operation: &VirtualMigrationPersistenceCommand,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    for region_id in &operation.request.source_region_ids {
        for family in [
            VirtualStateFamily::Regions,
            VirtualStateFamily::ActiveRegions,
        ] {
            require_virtual_local_read(source, required, family, region_id)?;
        }
    }
    if source.regions.keys().collect::<BTreeSet<_>>()
        != operation.request.source_region_ids.iter().collect()
        || source.active_regions.keys().collect::<BTreeSet<_>>()
            != operation.request.source_region_ids.iter().collect()
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual migration provider preflight requires every exact active source region"
                .to_owned(),
        )
        .into());
    }
    Ok(())
}

fn preflight_virtual_compaction(
    operation: &VirtualCompactionPersistenceCommand,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Regions,
        &operation.command.region_id,
    )?;
    for work_id in &operation.command.work_ids {
        require_virtual_local_read(source, required, VirtualStateFamily::Work, work_id)?;
    }
    for occurrence_id in &operation.command.occurrence_ids {
        require_virtual_local_read(
            source,
            required,
            VirtualStateFamily::Occurrences,
            occurrence_id,
        )?;
    }
    if source.regions.keys().collect::<BTreeSet<_>>()
        != BTreeSet::from([&operation.command.region_id])
        || source.work.keys().collect::<BTreeSet<_>>()
            != operation.command.work_ids.iter().collect()
        || source.occurrences.keys().collect::<BTreeSet<_>>()
            != operation.command.occurrence_ids.iter().collect()
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual compaction provider preflight requires its exact hot closure".to_owned(),
        )
        .into());
    }
    Ok(())
}

fn preflight_virtual_rehydration(
    operation: &VirtualRehydrationPersistenceCommand,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Certificates,
        &operation.command.certificate_id,
    )?;
    let Some(certificate) = source.certificates.get(&operation.command.certificate_id) else {
        return Err(ProtocolError::IllegalTransition(
            "Virtual rehydration provider preflight requires its exact certificate".to_owned(),
        )
        .into());
    };
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Regions,
        &certificate.certificate.summary.region_id,
    )?;
    for occurrence_id in &operation.command.occurrence_ids {
        require_virtual_local_read(
            source,
            required,
            VirtualStateFamily::Occurrences,
            occurrence_id,
        )?;
    }
    if source.regions.keys().collect::<BTreeSet<_>>()
        != BTreeSet::from([&certificate.certificate.summary.region_id])
        || !source
            .occurrences
            .keys()
            .all(|occurrence_id| operation.command.occurrence_ids.contains(occurrence_id))
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual rehydration provider preflight changed certificate closure or loaded orphan hot occurrences"
                .to_owned(),
        )
        .into());
    }
    Ok(())
}

struct ReducedVirtualOperation {
    evidence: VirtualPersistenceEvidence,
    current: VirtualCurrentDraft,
    mutations: Vec<VirtualStateMutation>,
    outcome: VirtualPersistenceOutcome,
    artifacts: Vec<ArtifactRecord>,
    archive_pin: Option<ResourcePinReceipt>,
    archive_release: Option<ResourceReleaseReceipt>,
}

/// Prepare one closed Virtual transition from exact pinned family reads.
///
/// Durable retries this pure function after satisfying each returned
/// [`VirtualPreparationError::ReadRequired`] from the unchanged pinned
/// `StateRoot`. Provider products may introduce additional exact keys, but
/// those keys are still read and verified before any postcondition can be
/// returned. Extra reads are rejected so an unbounded or cross-command view
/// cannot become hidden reducer authority.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn prepare_virtual(
    command: &VirtualPersistenceCommand,
    authority: &VirtualReductionAuthority,
) -> VirtualPreparationResult<VirtualReduction> {
    command.verify()?;
    authority.source.verify_bounds()?;
    if authority.source.scheduler_id != command.scheduler_id() {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual command scheduler does not match its exact pinned source".to_owned(),
        )
        .into());
    }
    let mut required = BTreeSet::new();
    require_virtual_operation_reads(command, authority, &mut required)?;
    let reduction = reduce_virtual(command.clone(), authority.clone())?;
    for mutation in &reduction.mutations.operations {
        let family = mutation.family();
        let storage_key = mutation.storage_key()?;
        required.insert((family, storage_key.clone()));
        authority.source.require_read(family, storage_key)?;
    }
    if authority.source.lookups != required {
        return Err(ProtocolError::IllegalTransition(
            "Virtual preparation source contains an orphan exact family read".to_owned(),
        )
        .into());
    }
    Ok(reduction)
}

fn require_virtual_operation_reads(
    command: &VirtualPersistenceCommand,
    authority: &VirtualReductionAuthority,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    let source = &authority.source;
    match (&command.operation, &authority.operation) {
        (VirtualPersistenceOperation::Initialize(_), VirtualOperationAuthority::Initialize) => {}
        (
            VirtualPersistenceOperation::Materialize(operation),
            VirtualOperationAuthority::Materialize { page, .. },
        ) => require_virtual_materialization_reads(operation, page, source, required)?,
        (
            VirtualPersistenceOperation::ActivateWait(_),
            VirtualOperationAuthority::ActivateWait { receipt, .. },
        ) => require_virtual_activation_reads(receipt, source, required)?,
        (
            VirtualPersistenceOperation::Resolve(operation),
            VirtualOperationAuthority::Resolve { .. },
        ) => {
            require_virtual_resolution_reads(
                source,
                required,
                &operation.command.work_id,
                operation.command.epoch,
                resolution_park_reason(&operation.command.resolution),
            )?;
        }
        (
            VirtualPersistenceOperation::MigrateRegion(_),
            VirtualOperationAuthority::MigrateRegion { command, .. },
        ) => require_virtual_migration_reads(&command.plan, source, required)?,
        (
            VirtualPersistenceOperation::Compact(operation),
            VirtualOperationAuthority::Compact { .. },
        ) => require_virtual_compaction_reads(&operation.command, source, required)?,
        (
            VirtualPersistenceOperation::Rehydrate(operation),
            VirtualOperationAuthority::Rehydrate { .. },
        ) => require_virtual_rehydration_reads(&operation.command, source, required)?,
        (
            VirtualPersistenceOperation::Claim(operation),
            VirtualOperationAuthority::Claim { .. },
        ) => {
            require_virtual_claim_reads(operation, source, required)?;
        }
        (
            VirtualPersistenceOperation::RenewLease(operation),
            VirtualOperationAuthority::RenewLease { .. },
        ) => require_virtual_renewal_reads(&operation.command, source, required)?,
        (
            VirtualPersistenceOperation::Recover(operation),
            VirtualOperationAuthority::Recover { .. },
        ) => {
            require_virtual_resolution_reads(
                source,
                required,
                &operation.command.work_id,
                operation.command.expected_epoch,
                resolution_park_reason(&operation.command.resolution),
            )?;
        }
        (
            VirtualPersistenceOperation::SetRunWeight(operation),
            VirtualOperationAuthority::SetRunWeight,
        ) => {
            require_virtual_local_read(
                source,
                required,
                VirtualStateFamily::Runs,
                &operation.command.run_id,
            )?;
        }
        (
            VirtualPersistenceOperation::RetireArchive(operation),
            VirtualOperationAuthority::RetireArchive { .. },
        ) => require_virtual_retirement_reads(&operation.command, source, required)?,
        _ => {
            return Err(ProtocolError::IllegalTransition(
                "Virtual semantic command received the wrong exact operation authority".to_owned(),
            )
            .into());
        }
    }
    Ok(())
}

fn require_virtual_materialization_reads(
    operation: &VirtualMaterializationCommand,
    page: &MaterializedPage,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    for family in [
        VirtualStateFamily::Regions,
        VirtualStateFamily::ActiveRegions,
    ] {
        require_virtual_local_read(source, required, family, &operation.region_id)?;
    }
    for item in &page.items {
        require_virtual_local_read(source, required, VirtualStateFamily::Work, &item.work_id)?;
    }
    Ok(())
}

fn require_virtual_activation_reads(
    receipt: &WaitActivationReceipt,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    let current = require_virtual_current(source)?;
    let reasons = receipt
        .applied_wait_ids
        .iter()
        .filter(|wait_id| {
            current
                .body
                .frontier
                .wait_activations
                .contains_key(*wait_id)
        })
        .map(|wait_id| ParkReason::Wait {
            key: wait_id.clone(),
        })
        .collect::<BTreeSet<_>>();
    let mut work_ids = BTreeSet::new();
    for reason in &reasons {
        work_ids.extend(require_parked_index_chain(source, required, reason)?);
    }
    for work_id in work_ids {
        require_virtual_local_read(source, required, VirtualStateFamily::Parked, &work_id)?;
        require_virtual_local_read(source, required, VirtualStateFamily::Work, &work_id)?;
    }
    Ok(())
}

fn require_virtual_migration_reads(
    plan: &RegionMigrationPlan,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    for region_id in plan
        .expected_sources
        .keys()
        .chain(plan.targets.iter().map(|target| &target.region_id))
    {
        for family in [
            VirtualStateFamily::Regions,
            VirtualStateFamily::ActiveRegions,
        ] {
            require_virtual_local_read(source, required, family, region_id)?;
        }
    }
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Migrations,
        &plan.migration_id,
    )
}

fn require_virtual_compaction_reads(
    command: &VirtualCompactionCommand,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Regions,
        &command.region_id,
    )?;
    for work_id in &command.work_ids {
        require_virtual_local_read(source, required, VirtualStateFamily::Work, work_id)?;
    }
    for occurrence_id in &command.occurrence_ids {
        require_virtual_local_read(
            source,
            required,
            VirtualStateFamily::Occurrences,
            occurrence_id,
        )?;
    }
    Ok(())
}

fn require_virtual_rehydration_reads(
    command: &VirtualRehydrationCommand,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Certificates,
        &command.certificate_id,
    )?;
    if let Some(certificate) = source.certificates.get(&command.certificate_id) {
        require_virtual_local_read(
            source,
            required,
            VirtualStateFamily::Regions,
            &certificate.certificate.summary.region_id,
        )?;
    }
    for occurrence_id in &command.occurrence_ids {
        require_virtual_local_read(
            source,
            required,
            VirtualStateFamily::Occurrences,
            occurrence_id,
        )?;
    }
    Ok(())
}

fn require_virtual_renewal_reads(
    command: &VirtualLeaseRenewalCommand,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    require_virtual_local_read(source, required, VirtualStateFamily::Work, &command.work_id)?;
    if let Some(current) = &source.current
        && let Some(claim) = current.body.frontier.active.get(&command.work_id)
    {
        require_virtual_local_read(
            source,
            required,
            VirtualStateFamily::Occurrences,
            &claim.occurrence_id,
        )?;
    }
    Ok(())
}

fn require_virtual_retirement_reads(
    command: &VirtualArchiveRetirementCommand,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Certificates,
        &command.certificate_id,
    )?;
    if let Some(certificate) = source.certificates.get(&command.certificate_id) {
        require_virtual_local_read(
            source,
            required,
            VirtualStateFamily::Regions,
            &certificate.certificate.summary.region_id,
        )?;
    }
    Ok(())
}

fn require_virtual_claim_reads(
    persistence: &VirtualClaimPersistenceCommand,
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
) -> VirtualPreparationResult<()> {
    let current = require_virtual_current(source)?;
    if current.body.frontier.active.len() < current.body.limits.max_active {
        for run_id in current.body.frontier.ready.keys() {
            require_virtual_local_read(source, required, VirtualStateFamily::Runs, run_id)?;
        }
    }
    let preview = preview_virtual_claim_loaded(persistence, source)?;
    let Some(preview) = preview else {
        return Ok(());
    };
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Work,
        &preview.item.work_id,
    )?;
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Regions,
        &preview.item.region_id,
    )?;
    if let Some(work) = source.work.get(&preview.item.work_id) {
        require_virtual_local_read(
            source,
            required,
            VirtualStateFamily::Occurrences,
            &preview.occurrence_id(work)?,
        )?;
    }
    Ok(())
}

fn require_virtual_resolution_reads(
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
    work_id: &str,
    epoch: u64,
    park_reason: Option<&ParkReason>,
) -> VirtualPreparationResult<()> {
    require_virtual_local_read(source, required, VirtualStateFamily::Work, work_id)?;
    let occurrence_id = cymule_core::content_id(VIRTUAL_WORK_OCCURRENCE_VERSION, &(work_id, epoch))
        .map_err(ProtocolError::from)?;
    require_virtual_local_read(
        source,
        required,
        VirtualStateFamily::Occurrences,
        &occurrence_id,
    )?;
    if let Some(reason) = park_reason {
        require_virtual_local_read(source, required, VirtualStateFamily::Parked, work_id)?;
        let _ = require_parked_index_chain(source, required, reason)?;
    }
    Ok(())
}

fn require_parked_index_chain(
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
    reason: &ParkReason,
) -> VirtualPreparationResult<BTreeSet<String>> {
    let mut page = 0_u64;
    let mut work_ids = BTreeSet::new();
    loop {
        let local_key = parked_index_local_key_for(reason, page)?;
        require_virtual_local_read(
            source,
            required,
            VirtualStateFamily::ParkedIndex,
            &local_key,
        )?;
        let Some(current) = source.parked_index.get(&local_key) else {
            return Ok(work_ids);
        };
        work_ids.extend(current.work_ids.iter().cloned());
        let Some(next_page) = current.next_page else {
            return Ok(work_ids);
        };
        if next_page
            != page.checked_add(1).ok_or_else(|| {
                ProtocolError::Validation("Virtual parked-index page overflowed".to_owned())
            })?
        {
            return Err(ProtocolError::IllegalTransition(
                "Virtual parked-index chain skipped its exact successor page".to_owned(),
            )
            .into());
        }
        page = next_page;
    }
}

fn require_virtual_local_read(
    source: &VirtualKeyedSource,
    required: &mut BTreeSet<(VirtualStateFamily, String)>,
    family: VirtualStateFamily,
    local_key: &str,
) -> VirtualPreparationResult<()> {
    let storage_key = virtual_state_storage_key(&source.scheduler_id, family, local_key)?;
    required.insert((family, storage_key.clone()));
    source.require_read(family, storage_key)
}

fn resolution_park_reason(resolution: &WorkResolution) -> Option<&ParkReason> {
    match resolution {
        WorkResolution::Retry {
            next_reason: Some(reason),
            ..
        }
        | WorkResolution::Parked { reason } => Some(reason),
        WorkResolution::Succeeded { .. }
        | WorkResolution::Failed { .. }
        | WorkResolution::Retry {
            next_reason: None, ..
        }
        | WorkResolution::Cancelled { .. } => None,
    }
}

/// Purely reduce one closed semantic command against exact Durable-loaded
/// keyed authority.
///
/// This function does not write storage and is not a persistence capability.
/// The Durable coordinator is the only public committer: it resolves the
/// non-serializable authority from its current M1/StateRoot/Resource snapshot,
/// applies the returned typed mutations, derives roots, calls
/// [`VirtualReduction::finish`], and commits that exact postcondition in one
/// compare-and-swap operation.
fn reduce_virtual(
    command: VirtualPersistenceCommand,
    authority: VirtualReductionAuthority,
) -> ProtocolResult<VirtualReduction> {
    command.verify()?;
    let persistence_id = command.persistence_id.clone();
    authority.source.verify_bounds()?;
    let parent_current_id = authority
        .source
        .current
        .as_ref()
        .map(|current| current.current_id.clone());
    if let Some(current) = &authority.source.current
        && current.body.scheduler_id != command.scheduler_id()
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual command scheduler does not match its exact parent current".to_owned(),
        ));
    }
    let reduced = reduce_virtual_operation(
        &command,
        &persistence_id,
        &authority.source,
        authority.operation,
    )?;
    let mutations = VirtualMutationSet::new(reduced.mutations)?;
    let reduction = VirtualReduction {
        command,
        parent_current_id,
        evidence: reduced.evidence,
        current: reduced.current,
        mutations,
        outcome: reduced.outcome,
        artifacts: reduced.artifacts,
        archive_pin: reduced.archive_pin,
        archive_release: reduced.archive_release,
    };
    verify_persistence_evidence(&reduction.command, &reduction.evidence)?;
    verify_virtual_outcome(&reduction.command, &reduction.evidence, &reduction.outcome)?;
    verify_persistence_artifacts(
        &reduction.command,
        &reduction.evidence,
        &reduction.artifacts,
    )?;
    Ok(reduction)
}

fn reduce_virtual_operation(
    command: &VirtualPersistenceCommand,
    persistence_id: &str,
    source: &VirtualKeyedSource,
    operation: VirtualOperationAuthority,
) -> ProtocolResult<ReducedVirtualOperation> {
    let reduced = match (&command.operation, operation) {
        (
            VirtualPersistenceOperation::Initialize(operation),
            VirtualOperationAuthority::Initialize,
        ) => reduce_virtual_initialization(operation, source)?,
        (
            VirtualPersistenceOperation::Materialize(operation),
            VirtualOperationAuthority::Materialize {
                selection,
                page,
                archived_work_proofs,
            },
        ) => reduce_virtual_materialization(
            operation,
            source,
            &selection,
            page,
            archived_work_proofs,
        )?,
        (
            VirtualPersistenceOperation::ActivateWait(operation),
            VirtualOperationAuthority::ActivateWait { receipt, result },
        ) => reduce_virtual_activation(operation, source, receipt, result)?,
        (
            VirtualPersistenceOperation::Resolve(operation),
            VirtualOperationAuthority::Resolve { clock },
        ) => reduce_virtual_resolution(operation, source, &clock, false)?,
        (
            VirtualPersistenceOperation::MigrateRegion(operation),
            VirtualOperationAuthority::MigrateRegion {
                command,
                coverage_evidence,
                target_source_artifacts,
            },
        ) => reduce_virtual_migration(
            operation,
            source,
            command,
            coverage_evidence,
            target_source_artifacts,
        )?,
        (
            VirtualPersistenceOperation::Compact(operation),
            VirtualOperationAuthority::Compact {
                manifest,
                archive,
                archive_pin,
            },
        ) => reduce_virtual_compaction(operation, source, manifest, archive, archive_pin)?,
        (
            VirtualPersistenceOperation::Rehydrate(operation),
            VirtualOperationAuthority::Rehydrate { occurrences },
        ) => reduce_virtual_rehydration(operation, source, occurrences)?,
        (
            VirtualPersistenceOperation::Claim(operation),
            VirtualOperationAuthority::Claim {
                clock,
                lease,
                execution,
                evolution_selection,
            },
        ) => reduce_virtual_claim(
            operation,
            persistence_id,
            source,
            clock,
            &lease,
            &execution,
            evolution_selection,
        )?,
        (
            VirtualPersistenceOperation::RenewLease(operation),
            VirtualOperationAuthority::RenewLease { clock, lease },
        ) => reduce_virtual_lease_renewal(operation, source, clock, lease)?,
        (
            VirtualPersistenceOperation::Recover(operation),
            VirtualOperationAuthority::Recover { clock },
        ) => reduce_virtual_recovery(operation, source, clock)?,
        (
            VirtualPersistenceOperation::SetRunWeight(operation),
            VirtualOperationAuthority::SetRunWeight,
        ) => reduce_virtual_run_weight(operation, source)?,
        (
            VirtualPersistenceOperation::RetireArchive(operation),
            VirtualOperationAuthority::RetireArchive { release, receipt },
        ) => reduce_virtual_archive_retirement(operation, source, &release, receipt)?,
        _ => {
            return Err(ProtocolError::IllegalTransition(
                "Virtual semantic command received the wrong exact operation authority".to_owned(),
            ));
        }
    };
    Ok(reduced)
}

fn reduce_virtual_initialization(
    command: &VirtualInitializationCommand,
    source: &VirtualKeyedSource,
) -> ProtocolResult<ReducedVirtualOperation> {
    if source.current.is_some() || !source_is_empty(source) {
        return Err(ProtocolError::IllegalTransition(
            "Virtual initialization requires an empty scheduler authority".to_owned(),
        ));
    }
    let mut mutations = Vec::new();
    for region in &command.regions {
        mutations.push(VirtualStateMutation::Regions {
            before: None,
            after: Some(VirtualRegionCurrent {
                leaf_version: VIRTUAL_REGION_CURRENT_VERSION.to_owned(),
                scheduler_id: command.scheduler_id.clone(),
                region: region.clone(),
                lifecycle: VirtualRegionLifecycle::Active,
                hot_work_count: 0,
                hot_occurrence_count: 0,
                compaction_certificate_id: None,
            }),
        });
        if !region.cursor.exhausted {
            mutations.push(VirtualStateMutation::ActiveRegions {
                before: None,
                after: Some(VirtualActiveRegionCurrent {
                    leaf_version: VIRTUAL_ACTIVE_REGION_CURRENT_VERSION.to_owned(),
                    scheduler_id: command.scheduler_id.clone(),
                    region_id: region.region_id.clone(),
                }),
            });
        }
    }
    for run in &command.runs {
        mutations.push(VirtualStateMutation::Runs {
            before: None,
            after: Some(VirtualRunCurrent {
                leaf_version: VIRTUAL_RUN_CURRENT_VERSION.to_owned(),
                scheduler_id: command.scheduler_id.clone(),
                run_id: run.run_id.clone(),
                execution: run.execution.clone(),
                weight: 1,
                deficit: 0,
            }),
        });
    }
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::None,
        current: VirtualCurrentDraft {
            scheduler_id: command.scheduler_id.clone(),
            limits: command.limits,
            scheduling_policy: command.scheduling_policy,
            archive: command.archive.clone(),
            frontier: VirtualFrontierCurrent {
                ready: BTreeMap::new(),
                active: BTreeMap::new(),
                dispatch_sequence: 0,
                ready_since: BTreeMap::new(),
                wait_activations: BTreeMap::new(),
                last_run: None,
                last_region: None,
            },
            archived_work_index_root_digest: virtual_work_index_empty_root(),
            archived_command_index_root_digest: virtual_command_index_empty_root(),
            counts: VirtualCurrentCounts {
                regions: command.regions.len() as u64,
                active_regions: command
                    .regions
                    .iter()
                    .filter(|region| !region.cursor.exhausted)
                    .count() as u64,
                parked: 0,
                hot_work: 0,
                hot_occurrences: 0,
                runs: command.runs.len() as u64,
                migrations: 0,
                certificates: 0,
            },
        },
        mutations,
        outcome: VirtualPersistenceOutcome::Initialized {
            region_count: command.regions.len() as u64,
        },
        artifacts: command.source_artifacts.clone(),
        archive_pin: None,
        archive_release: None,
    })
}

fn reduce_virtual_materialization(
    command: &VirtualMaterializationCommand,
    source: &VirtualKeyedSource,
    selection: &VirtualActiveRegionSelectionProof,
    page: MaterializedPage,
    archived_work_proofs: BTreeMap<String, VirtualArchiveWorkProof>,
) -> ProtocolResult<ReducedVirtualOperation> {
    let (current, before, active_before) =
        validate_virtual_materialization(command, source, selection, &page, &archived_work_proofs)?;
    let mut next = before.clone();
    next.region.cursor = page.next_cursor.clone();
    next.hot_work_count = checked_exact_add(
        "Virtual region hot work count",
        next.hot_work_count,
        page.items.len() as u64,
    )?;
    let mut frontier = current.body.frontier.clone();
    let mut mutations = vec![VirtualStateMutation::Regions {
        before: Some(before),
        after: Some(next),
    }];
    if page.next_cursor.exhausted {
        mutations.push(VirtualStateMutation::ActiveRegions {
            before: Some(active_before),
            after: None,
        });
    }
    for item in &page.items {
        if item.region_id != command.region_id
            || item.run_id != source.regions[&command.region_id].region.run_id
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual materialized work escaped its exact region or Run".to_owned(),
            ));
        }
        archived_work_proofs[&item.work_id]
            .verify(&current.body.archived_work_index_root_digest)?;
        insert_ready_frontier(&mut frontier, item.clone())?;
        mutations.push(VirtualStateMutation::Work {
            before: None,
            after: Some(VirtualWorkCurrent {
                leaf_version: VIRTUAL_WORK_CURRENT_VERSION.to_owned(),
                scheduler_id: command.scheduler_id.clone(),
                item: item.clone(),
                max_epoch: 0,
                latest_occurrence_id: None,
                placement: VirtualWorkPlacement::Ready,
            }),
        });
    }
    let mut counts = current.body.counts;
    if page.next_cursor.exhausted {
        counts.active_regions =
            checked_exact_sub("Virtual active region count", counts.active_regions, 1)?;
        if frontier.last_region.as_deref() == Some(command.region_id.as_str()) {
            frontier.last_region = None;
        }
    } else {
        frontier.last_region = Some(command.region_id.clone());
    }
    counts.hot_work = checked_exact_add(
        "Virtual hot work count",
        counts.hot_work,
        page.items.len() as u64,
    )?;
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::Materialized {
            page: page.clone(),
            archived_work_proofs,
        },
        current: draft_from_current(current, frontier, counts),
        mutations,
        outcome: VirtualPersistenceOutcome::Materialized {
            region_id: command.region_id.clone(),
            materialized: page.items.len() as u64,
        },
        artifacts: page.artifacts,
        archive_pin: None,
        archive_release: None,
    })
}

fn validate_virtual_materialization<'a>(
    command: &VirtualMaterializationCommand,
    source: &'a VirtualKeyedSource,
    selection: &VirtualActiveRegionSelectionProof,
    page: &MaterializedPage,
    archived_work_proofs: &BTreeMap<String, VirtualArchiveWorkProof>,
) -> ProtocolResult<(
    &'a VirtualCurrent,
    VirtualRegionCurrent,
    VirtualActiveRegionCurrent,
)> {
    let current = require_virtual_current(source)?;
    selection.verify_for(current)?;
    let selected_key = selection.selected_storage_key().ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Virtual materialization cannot run with an empty ActiveRegions root".to_owned(),
        )
    })?;
    if selected_key != virtual_active_region_key(&command.scheduler_id, &command.region_id)? {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual materialization intent does not match Durable's ordered region selection"
                .to_owned(),
        ));
    }
    require_only_source_families(
        source,
        &[
            VirtualStateFamily::Regions,
            VirtualStateFamily::ActiveRegions,
            VirtualStateFamily::Work,
        ],
    )?;
    let selected = BTreeSet::from([command.region_id.clone()]);
    if source.regions.keys().cloned().collect::<BTreeSet<_>>() != selected
        || source
            .active_regions
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != selected
        || !source.work.is_empty()
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual materialization source is missing its exact region or found hot work collision"
                .to_owned(),
        ));
    }
    let before = source.regions[&command.region_id].clone();
    let active_before = source.active_regions[&command.region_id].clone();
    if before.lifecycle != VirtualRegionLifecycle::Active
        || before.compaction_certificate_id.is_some()
        || before.region.source != command.expected_source
        || before.region.cursor != command.expected_cursor
        || before.region.cursor.exhausted
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual materialization region is retired, compacted, exhausted, or stale".to_owned(),
        ));
    }
    verify_materialization_evidence(command, page, archived_work_proofs)?;
    let available = current
        .body
        .limits
        .max_materialized
        .checked_sub(virtual_materialized_count(current)?)
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(
                "Virtual materialized frontier already exceeds its configured bound".to_owned(),
            )
        })?;
    if page.items.len() > available.min(current.body.limits.materialize_batch) {
        return Err(ProtocolError::Validation(
            "Virtual source page exceeds the exact reducer-selected materialization limit"
                .to_owned(),
        ));
    }
    Ok((current, before, active_before))
}

fn reduce_virtual_activation(
    command: &VirtualActivationCommand,
    source: &VirtualKeyedSource,
    receipt: WaitActivationReceipt,
    result: ArtifactRecord,
) -> ProtocolResult<ReducedVirtualOperation> {
    let validated = validate_virtual_activation(command, source, &receipt, &result)?;
    let mut frontier = validated.current.body.frontier.clone();
    for reason in &validated.reasons {
        let ParkReason::Wait { key } = reason else {
            return Err(ProtocolError::Integrity {
                code: "virtual_activation_reason_kind_changed".to_owned(),
                message: "Virtual M1 activation selected a non-Wait park reason".to_owned(),
            });
        };
        frontier
            .wait_activations
            .remove(key)
            .ok_or_else(|| ProtocolError::Integrity {
                code: "virtual_wait_capacity_missing".to_owned(),
                message: format!(
                    "Virtual M1 activation lost the capacity directory for Wait {key}"
                ),
            })?;
    }
    let mut mutations = validated
        .pages
        .into_iter()
        .map(|page| VirtualStateMutation::ParkedIndex {
            before: Some(page),
            after: None,
        })
        .collect::<Vec<_>>();
    for work_id in &validated.work_ids {
        let parked = source.parked[work_id].clone();
        let before = source.work[work_id].clone();
        if !validated.reasons.contains(&parked.parked.reason)
            || before.placement != VirtualWorkPlacement::Parked
            || before.item != parked.parked.item
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual activation parked and work leaves disagree".to_owned(),
            ));
        }
        insert_ready_frontier(&mut frontier, parked.parked.item.clone())?;
        let mut after = before.clone();
        after.placement = VirtualWorkPlacement::Ready;
        mutations.push(VirtualStateMutation::Parked {
            before: Some(parked),
            after: None,
        });
        mutations.push(VirtualStateMutation::Work {
            before: Some(before),
            after: Some(after),
        });
    }
    let mut counts = validated.current.body.counts;
    counts.parked = checked_exact_sub(
        "Virtual parked count",
        counts.parked,
        validated.work_ids.len() as u64,
    )?;
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::Activated {
            receipt,
            result: result.clone(),
        },
        current: draft_from_current(validated.current, frontier, counts),
        mutations,
        outcome: VirtualPersistenceOutcome::Activated {
            activation_id: command.activation_id.clone(),
            woken: validated.work_ids.len() as u64,
        },
        artifacts: vec![result],
        archive_pin: None,
        archive_release: None,
    })
}

struct ValidatedVirtualActivation<'a> {
    current: &'a VirtualCurrent,
    reasons: BTreeSet<ParkReason>,
    work_ids: BTreeSet<String>,
    pages: Vec<VirtualParkedIndexPage>,
}

fn validate_virtual_activation<'a>(
    command: &VirtualActivationCommand,
    source: &'a VirtualKeyedSource,
    receipt: &WaitActivationReceipt,
    result: &ArtifactRecord,
) -> ProtocolResult<ValidatedVirtualActivation<'a>> {
    let current = require_virtual_current(source)?;
    require_only_source_families(
        source,
        &[
            VirtualStateFamily::Parked,
            VirtualStateFamily::ParkedIndex,
            VirtualStateFamily::Work,
        ],
    )?;
    receipt.verify()?;
    verify_exact_artifact_record(
        result,
        &receipt.activation.result,
        "Virtual wait activation",
    )?;
    if receipt.activation.activation_id != command.activation_id {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual activation authority changed its admitted activation identity".to_owned(),
        ));
    }
    let wait_ids = receipt
        .applied_wait_ids
        .iter()
        .filter(|wait_id| {
            current
                .body
                .frontier
                .wait_activations
                .contains_key(*wait_id)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let reasons = wait_ids
        .iter()
        .map(|wait_id| ParkReason::Wait {
            key: wait_id.clone(),
        })
        .collect::<BTreeSet<_>>();
    if source
        .parked_index
        .values()
        .any(|page| !reasons.contains(&page.reason))
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual activation source contains an orphan parked-index reason".to_owned(),
        ));
    }
    let mut work_ids = BTreeSet::new();
    let mut pages = Vec::new();
    for wait_id in &wait_ids {
        let reason = ParkReason::Wait {
            key: wait_id.clone(),
        };
        let reason_pages = exact_parked_index_pages(source, &reason)?;
        let capacity = recompute_virtual_wait_activation_capacity(wait_id, &reason_pages, source)?;
        if current.body.frontier.wait_activations.get(wait_id) != Some(&capacity) {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual Wait activation capacity differs from its exact keyed leaves".to_owned(),
            ));
        }
        for page in reason_pages {
            for work_id in &page.work_ids {
                if !work_ids.insert(work_id.clone()) {
                    return Err(ProtocolError::IllegalTransition(
                        "Virtual activation indexed one work identity under multiple Waits"
                            .to_owned(),
                    ));
                }
            }
            pages.push(page.clone());
        }
    }
    if source.parked.keys().cloned().collect::<BTreeSet<_>>() != work_ids
        || source.work.keys().cloned().collect::<BTreeSet<_>>() != work_ids
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual activation did not load exactly its indexed parked work".to_owned(),
        ));
    }
    Ok(ValidatedVirtualActivation {
        current,
        reasons,
        work_ids,
        pages,
    })
}

fn exact_parked_index_pages<'a>(
    source: &'a VirtualKeyedSource,
    reason: &ParkReason,
) -> ProtocolResult<Vec<&'a VirtualParkedIndexPage>> {
    let mut pages = source
        .parked_index
        .values()
        .filter(|page| &page.reason == reason)
        .collect::<Vec<_>>();
    pages.sort_by_key(|page| page.page);
    for (index, page) in pages.iter().enumerate() {
        let expected_page = index as u64;
        let expected_next = (index + 1 < pages.len()).then_some(expected_page + 1);
        if page.page != expected_page || page.next_page != expected_next {
            return Err(ProtocolError::IllegalTransition(
                "Virtual parked-index pages are not a complete consecutive chain".to_owned(),
            ));
        }
    }
    Ok(pages)
}

fn recompute_virtual_wait_activation_capacity(
    wait_id: &str,
    pages: &[&VirtualParkedIndexPage],
    source: &VirtualKeyedSource,
) -> ProtocolResult<VirtualWaitActivationCapacity> {
    validate_content_id("Virtual parked Wait", wait_id)?;
    if pages.is_empty() {
        return Err(ProtocolError::Integrity {
            code: "virtual_wait_index_missing".to_owned(),
            message: format!("Virtual Wait {wait_id} has capacity authority but no index page"),
        });
    }
    let reason = ParkReason::Wait {
        key: wait_id.to_owned(),
    };
    let mut work_ids = BTreeSet::new();
    let mut source_bytes = 0_u64;
    let mut mutation_bytes = 0_u64;
    for page in pages {
        if page.reason != reason {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual Wait capacity page changed its reason".to_owned(),
            ));
        }
        for work_id in &page.work_ids {
            if !work_ids.insert(work_id.clone()) {
                return Err(ProtocolError::IllegalTransition(
                    "Virtual Wait capacity repeats a work identity".to_owned(),
                ));
            }
        }
        source_bytes = checked_exact_add(
            "Virtual Wait activation source bytes",
            source_bytes,
            virtual_index_page_source_bytes(page)?,
        )?;
        mutation_bytes = checked_exact_add(
            "Virtual Wait activation mutation bytes",
            mutation_bytes,
            virtual_index_page_delete_bytes(page)?,
        )?;
    }
    for work_id in &work_ids {
        let parked = source
            .parked
            .get(work_id)
            .ok_or_else(|| ProtocolError::Integrity {
                code: "virtual_wait_parked_leaf_missing".to_owned(),
                message: format!("Virtual Wait {wait_id} lost parked work {work_id}"),
            })?;
        let work = source
            .work
            .get(work_id)
            .ok_or_else(|| ProtocolError::Integrity {
                code: "virtual_wait_work_leaf_missing".to_owned(),
                message: format!("Virtual Wait {wait_id} lost work {work_id}"),
            })?;
        if parked.parked.reason != reason
            || parked.parked.item != work.item
            || work.placement != VirtualWorkPlacement::Parked
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual Wait capacity Parked and Work leaves disagree".to_owned(),
            ));
        }
        for leaf in [
            VirtualStateLeaf::Parked(parked.clone()),
            VirtualStateLeaf::Work(work.clone()),
        ] {
            source_bytes = checked_exact_add(
                "Virtual Wait activation source bytes",
                source_bytes,
                virtual_activation_source_leaf_bytes(&leaf)?,
            )?;
        }
        let mut ready_work = work.clone();
        ready_work.placement = VirtualWorkPlacement::Ready;
        for mutation in [
            VirtualStateMutation::Parked {
                before: Some(parked.clone()),
                after: None,
            },
            VirtualStateMutation::Work {
                before: Some(work.clone()),
                after: Some(ready_work),
            },
        ] {
            mutation_bytes = checked_exact_add(
                "Virtual Wait activation mutation bytes",
                mutation_bytes,
                virtual_activation_mutation_bytes(&mutation)?,
            )?;
        }
    }
    let capacity = VirtualWaitActivationCapacity {
        work_items: u64::try_from(work_ids.len())
            .map_err(|error| ProtocolError::Validation(error.to_string()))?,
        index_pages: u64::try_from(pages.len())
            .map_err(|error| ProtocolError::Validation(error.to_string()))?,
        source_bytes,
        mutation_bytes,
    };
    capacity.verify()?;
    Ok(capacity)
}

fn reduce_virtual_resolution(
    command: &VirtualResolutionPersistenceCommand,
    source: &VirtualKeyedSource,
    clock: &ClockObservation,
    expired: bool,
) -> ProtocolResult<ReducedVirtualOperation> {
    let intent = VirtualResolutionIntent {
        work_id: &command.command.work_id,
        owner: &command.command.owner,
        epoch: command.command.epoch,
        lease_epoch: command.command.expected_lease_epoch,
        clock_ref: &command.command.clock,
        resolution: &command.command.resolution,
        require_expired: expired,
    };
    let (frontier, counts, mutations, occurrence) =
        apply_virtual_resolution(source, &intent, clock)?;
    let current = require_virtual_current(source)?;
    let receipt = WorkResolutionReceipt {
        command: command.command.clone(),
        occurrence,
    };
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::None,
        current: draft_from_current(current, frontier, counts),
        mutations,
        outcome: VirtualPersistenceOutcome::Resolved(receipt),
        artifacts: command.artifact.iter().cloned().collect(),
        archive_pin: None,
        archive_release: None,
    })
}

fn reduce_virtual_recovery(
    command: &VirtualRecoveryPersistenceCommand,
    source: &VirtualKeyedSource,
    clock: ClockObservation,
) -> ProtocolResult<ReducedVirtualOperation> {
    let intent = VirtualResolutionIntent {
        work_id: &command.command.work_id,
        owner: &command.command.expected_owner,
        epoch: command.command.expected_epoch,
        lease_epoch: command.command.expected_lease_epoch,
        clock_ref: &command.command.clock,
        resolution: &command.command.resolution,
        require_expired: true,
    };
    let (frontier, counts, mutations, occurrence) =
        apply_virtual_resolution(source, &intent, &clock)?;
    let current = require_virtual_current(source)?;
    let receipt = VirtualRecoveryReceipt {
        command: command.command.clone(),
        clock_observation: clock,
        occurrence,
    };
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::None,
        current: draft_from_current(current, frontier, counts),
        mutations,
        outcome: VirtualPersistenceOutcome::Recovered(receipt),
        artifacts: vec![command.artifact.clone()],
        archive_pin: None,
        archive_release: None,
    })
}

struct VirtualResolutionIntent<'a> {
    work_id: &'a str,
    owner: &'a str,
    epoch: u64,
    lease_epoch: u64,
    clock_ref: &'a ClockObservationRef,
    resolution: &'a WorkResolution,
    require_expired: bool,
}

fn apply_virtual_resolution(
    source: &VirtualKeyedSource,
    intent: &VirtualResolutionIntent<'_>,
    clock: &ClockObservation,
) -> ProtocolResult<(
    VirtualFrontierCurrent,
    VirtualCurrentCounts,
    Vec<VirtualStateMutation>,
    WorkOccurrence,
)> {
    let validated = validate_virtual_resolution(source, intent, clock)?;
    let mut occurrence = validated.before_occurrence.occurrence.clone();
    occurrence.lease_clock = intent.clock_ref.clone();
    let mut after_work = validated.before_work.clone();
    let mut frontier = validated.current.body.frontier.clone();
    frontier.active.remove(intent.work_id);
    let mut mutations = Vec::new();
    let mut counts = validated.current.body.counts;
    apply_virtual_resolution_disposition(
        source,
        validated.current,
        &validated.claim,
        intent.resolution,
        &mut VirtualResolutionChanges {
            occurrence: &mut occurrence,
            after_work: &mut after_work,
            frontier: &mut frontier,
            mutations: &mut mutations,
            counts: &mut counts,
        },
    )?;
    mutations.push(VirtualStateMutation::Work {
        before: Some(validated.before_work),
        after: Some(after_work),
    });
    mutations.push(VirtualStateMutation::Occurrences {
        before: Some(Box::new(validated.before_occurrence)),
        after: Some(Box::new(VirtualOccurrenceCurrent {
            leaf_version: VIRTUAL_OCCURRENCE_CURRENT_VERSION.to_owned(),
            scheduler_id: validated.current.body.scheduler_id.clone(),
            occurrence: occurrence.clone(),
        })),
    });
    Ok((frontier, counts, mutations, occurrence))
}

struct ValidatedVirtualResolution<'a> {
    current: &'a VirtualCurrent,
    claim: ClaimedWork,
    before_work: VirtualWorkCurrent,
    before_occurrence: VirtualOccurrenceCurrent,
}

struct VirtualResolutionChanges<'a> {
    occurrence: &'a mut WorkOccurrence,
    after_work: &'a mut VirtualWorkCurrent,
    frontier: &'a mut VirtualFrontierCurrent,
    mutations: &'a mut Vec<VirtualStateMutation>,
    counts: &'a mut VirtualCurrentCounts,
}

fn validate_virtual_resolution<'a>(
    source: &'a VirtualKeyedSource,
    intent: &VirtualResolutionIntent<'_>,
    clock: &ClockObservation,
) -> ProtocolResult<ValidatedVirtualResolution<'a>> {
    let current = require_virtual_current(source)?;
    require_only_source_families(
        source,
        &[
            VirtualStateFamily::Parked,
            VirtualStateFamily::ParkedIndex,
            VirtualStateFamily::Work,
            VirtualStateFamily::Occurrences,
        ],
    )?;
    let occurrence_id = cymule_core::content_id(
        VIRTUAL_WORK_OCCURRENCE_VERSION,
        &(intent.work_id, intent.epoch),
    )?;
    if source.work.keys().cloned().collect::<BTreeSet<_>>()
        != BTreeSet::from([intent.work_id.to_owned()])
        || source.occurrences.keys().cloned().collect::<BTreeSet<_>>()
            != BTreeSet::from([occurrence_id.clone()])
        || !source.parked.is_empty()
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual resolution did not load exactly its active work and occurrence".to_owned(),
        ));
    }
    let claim = current
        .body
        .frontier
        .active
        .get(intent.work_id)
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(format!(
                "Virtual active work {} is missing",
                intent.work_id
            ))
        })?;
    verify_clock_observation(intent.clock_ref, clock, &claim.lease.resource)?;
    verify_clock_timeline(&claim.lease.clock, intent.clock_ref)?;
    if claim.owner != intent.owner
        || claim.epoch != intent.epoch
        || claim.occurrence_id != occurrence_id
        || claim.lease.epoch != intent.lease_epoch
        || intent.require_expired != (clock.logical_time >= claim.lease.expires_at)
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual resolution has a stale owner, occurrence, lease, or expiry fence".to_owned(),
        ));
    }
    let before_work = source.work[intent.work_id].clone();
    let before_occurrence = source.occurrences[&occurrence_id].clone();
    if before_work.item != claim.item
        || before_work.placement != VirtualWorkPlacement::Active
        || before_work.latest_occurrence_id.as_deref() != Some(occurrence_id.as_str())
        || before_work.max_epoch != intent.epoch
        || before_occurrence.occurrence.state != WorkOccurrenceState::Running
        || before_occurrence.occurrence.owner != intent.owner
        || before_occurrence.occurrence.lease_epoch != intent.lease_epoch
        || before_occurrence.occurrence.execution_binding != claim.execution_binding
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual active frontier, work leaf, and occurrence leaf disagree".to_owned(),
        ));
    }
    Ok(ValidatedVirtualResolution {
        current,
        claim: claim.clone(),
        before_work,
        before_occurrence,
    })
}

fn apply_virtual_resolution_disposition(
    source: &VirtualKeyedSource,
    current: &VirtualCurrent,
    claim: &ClaimedWork,
    resolution: &WorkResolution,
    changes: &mut VirtualResolutionChanges<'_>,
) -> ProtocolResult<()> {
    match resolution {
        WorkResolution::Succeeded { result } => {
            changes.occurrence.state = WorkOccurrenceState::Succeeded;
            changes.occurrence.result = Some(result.clone());
            changes.after_work.placement = VirtualWorkPlacement::Terminal;
            if !source.parked_index.is_empty() {
                return Err(ProtocolError::IllegalTransition(
                    "terminal Virtual resolution loaded orphan parked-index leaves".to_owned(),
                ));
            }
        }
        WorkResolution::Failed { error } => {
            changes.occurrence.state = WorkOccurrenceState::Failed;
            changes.occurrence.error = Some(error.clone());
            changes.after_work.placement = VirtualWorkPlacement::Terminal;
            if !source.parked_index.is_empty() {
                return Err(ProtocolError::IllegalTransition(
                    "terminal Virtual resolution loaded orphan parked-index leaves".to_owned(),
                ));
            }
        }
        WorkResolution::Cancelled { reason } => {
            changes.occurrence.state = WorkOccurrenceState::Cancelled;
            changes.occurrence.error = Some(reason.clone());
            changes.after_work.placement = VirtualWorkPlacement::Terminal;
            if !source.parked_index.is_empty() {
                return Err(ProtocolError::IllegalTransition(
                    "terminal Virtual resolution loaded orphan parked-index leaves".to_owned(),
                ));
            }
        }
        WorkResolution::Retry {
            error,
            next_reason: None,
        } => {
            changes.occurrence.state = WorkOccurrenceState::RetryScheduled;
            changes.occurrence.error = Some(error.clone());
            changes.after_work.placement = VirtualWorkPlacement::Ready;
            if !source.parked_index.is_empty() {
                return Err(ProtocolError::IllegalTransition(
                    "ready Virtual retry loaded orphan parked-index leaves".to_owned(),
                ));
            }
            insert_ready_frontier(changes.frontier, claim.item.clone())?;
        }
        WorkResolution::Retry {
            error,
            next_reason: Some(reason),
        } => {
            changes.occurrence.state = WorkOccurrenceState::RetryScheduled;
            changes.occurrence.error = Some(error.clone());
            changes.occurrence.next_reason = Some(reason.clone());
            changes.after_work.placement = VirtualWorkPlacement::Parked;
            append_parked_work(
                source,
                current,
                &claim.item,
                reason,
                changes.after_work,
                changes.frontier,
                changes.mutations,
            )?;
            changes.counts.parked =
                checked_exact_add("Virtual parked count", changes.counts.parked, 1)?;
        }
        WorkResolution::Parked { reason } => {
            changes.occurrence.state = WorkOccurrenceState::Parked;
            changes.occurrence.next_reason = Some(reason.clone());
            changes.after_work.placement = VirtualWorkPlacement::Parked;
            append_parked_work(
                source,
                current,
                &claim.item,
                reason,
                changes.after_work,
                changes.frontier,
                changes.mutations,
            )?;
            changes.counts.parked =
                checked_exact_add("Virtual parked count", changes.counts.parked, 1)?;
        }
    }
    Ok(())
}

fn append_parked_work(
    source: &VirtualKeyedSource,
    current: &VirtualCurrent,
    item: &WorkItem,
    reason: &ParkReason,
    after_work: &VirtualWorkCurrent,
    frontier: &mut VirtualFrontierCurrent,
    mutations: &mut Vec<VirtualStateMutation>,
) -> ProtocolResult<()> {
    let scheduler_id = &current.body.scheduler_id;
    if source.parked.contains_key(&item.work_id) {
        return Err(ProtocolError::IllegalTransition(
            "Virtual resolution collided with existing parked work".to_owned(),
        ));
    }
    let pages = exact_parked_index_pages(source, reason)?;
    if source
        .parked_index
        .values()
        .any(|page| &page.reason != reason)
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual park transition loaded an orphan reason-index chain".to_owned(),
        ));
    }
    let total = pages.iter().map(|page| page.work_ids.len()).sum::<usize>();
    if total >= MAX_VIRTUAL_MUTATION_ITEMS {
        return Err(ProtocolError::Validation(
            "Virtual parked reason exceeds its bounded exact wake set".to_owned(),
        ));
    }
    let mut index_mutations = Vec::new();
    if let Some(last) = pages.last() {
        if last.work_ids.contains(&item.work_id) {
            return Err(ProtocolError::IllegalTransition(
                "Virtual parked-index chain repeats work identity".to_owned(),
            ));
        }
        if last.work_ids.len() < MAX_VIRTUAL_PARKED_INDEX_PAGE_ITEMS {
            let mut after = (*last).clone();
            after.work_ids.insert(item.work_id.clone());
            index_mutations.push(VirtualStateMutation::ParkedIndex {
                before: Some((*last).clone()),
                after: Some(after),
            });
        } else {
            let next_page = last.page.checked_add(1).ok_or_else(|| {
                ProtocolError::Validation("Virtual parked-index page overflowed".to_owned())
            })?;
            let mut previous = (*last).clone();
            previous.next_page = Some(next_page);
            index_mutations.push(VirtualStateMutation::ParkedIndex {
                before: Some((*last).clone()),
                after: Some(previous),
            });
            index_mutations.push(VirtualStateMutation::ParkedIndex {
                before: None,
                after: Some(VirtualParkedIndexPage {
                    page_version: VIRTUAL_PARKED_INDEX_PAGE_VERSION.to_owned(),
                    scheduler_id: scheduler_id.to_owned(),
                    reason: reason.clone(),
                    page: next_page,
                    work_ids: BTreeSet::from([item.work_id.clone()]),
                    next_page: None,
                }),
            });
        }
    } else {
        index_mutations.push(VirtualStateMutation::ParkedIndex {
            before: None,
            after: Some(VirtualParkedIndexPage {
                page_version: VIRTUAL_PARKED_INDEX_PAGE_VERSION.to_owned(),
                scheduler_id: scheduler_id.to_owned(),
                reason: reason.clone(),
                page: 0,
                work_ids: BTreeSet::from([item.work_id.clone()]),
                next_page: None,
            }),
        });
    }
    let parked = VirtualParkedCurrent {
        leaf_version: VIRTUAL_PARKED_CURRENT_VERSION.to_owned(),
        scheduler_id: scheduler_id.to_owned(),
        parked: ParkedWork {
            item: item.clone(),
            reason: reason.clone(),
        },
    };
    if let ParkReason::Wait { key } = reason {
        update_virtual_wait_activation_capacity(
            frontier,
            key,
            after_work,
            &parked,
            &index_mutations,
        )?;
    }
    mutations.extend(index_mutations);
    mutations.push(VirtualStateMutation::Parked {
        before: None,
        after: Some(parked),
    });
    Ok(())
}

fn virtual_activation_source_leaf_bytes(leaf: &VirtualStateLeaf) -> ProtocolResult<u64> {
    let storage_key = leaf.storage_key()?;
    u64::try_from(
        cymule_core::canonical_bytes(&(leaf.family(), storage_key.as_str(), Some(leaf)))?.len(),
    )
    .map_err(|error| ProtocolError::Validation(error.to_string()))
}

fn virtual_activation_mutation_bytes(mutation: &VirtualStateMutation) -> ProtocolResult<u64> {
    mutation.verify()?;
    u64::try_from(cymule_core::canonical_bytes(mutation)?.len())
        .map_err(|error| ProtocolError::Validation(error.to_string()))
}

fn virtual_index_page_source_bytes(page: &VirtualParkedIndexPage) -> ProtocolResult<u64> {
    virtual_activation_source_leaf_bytes(&VirtualStateLeaf::ParkedIndex(page.clone()))
}

fn virtual_index_page_delete_bytes(page: &VirtualParkedIndexPage) -> ProtocolResult<u64> {
    virtual_activation_mutation_bytes(&VirtualStateMutation::ParkedIndex {
        before: Some(page.clone()),
        after: None,
    })
}

fn update_virtual_wait_activation_capacity(
    frontier: &mut VirtualFrontierCurrent,
    wait_id: &str,
    after_work: &VirtualWorkCurrent,
    parked: &VirtualParkedCurrent,
    index_mutations: &[VirtualStateMutation],
) -> ProtocolResult<()> {
    validate_content_id("Virtual parked Wait", wait_id)?;
    after_work.verify()?;
    parked.verify()?;
    if after_work.item != parked.parked.item
        || after_work.placement != VirtualWorkPlacement::Parked
        || parked.parked.reason
            != (ParkReason::Wait {
                key: wait_id.to_owned(),
            })
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual Wait capacity received a different parked work projection".to_owned(),
        ));
    }
    let mut capacity =
        frontier
            .wait_activations
            .get(wait_id)
            .copied()
            .unwrap_or(VirtualWaitActivationCapacity {
                work_items: 0,
                index_pages: 0,
                source_bytes: 0,
                mutation_bytes: 0,
            });
    for mutation in index_mutations {
        apply_virtual_wait_index_capacity_mutation(&mut capacity, mutation)?;
    }
    add_virtual_wait_work_capacity(&mut capacity, after_work, parked)?;
    capacity.verify()?;
    frontier
        .wait_activations
        .insert(wait_id.to_owned(), capacity);
    let (_, source_items, _, mutation_bytes) = virtual_wait_activation_totals(frontier)?;
    if source_items > MAX_VIRTUAL_REDUCTION_SOURCE_ITEMS as u64
        || virtual_mutation_set_encoded_bytes(source_items, mutation_bytes)?
            > MAX_VIRTUAL_MUTATION_BYTES as u64
    {
        return Err(ProtocolError::Validation(
            "parking this work would make a legal Wait activation exceed its exact aggregate source or mutation bound"
                .to_owned(),
        ));
    }
    Ok(())
}

fn apply_virtual_wait_index_capacity_mutation(
    capacity: &mut VirtualWaitActivationCapacity,
    mutation: &VirtualStateMutation,
) -> ProtocolResult<()> {
    let VirtualStateMutation::ParkedIndex { before, after } = mutation else {
        return Err(ProtocolError::Validation(
            "Virtual Wait capacity received a non-index mutation".to_owned(),
        ));
    };
    if let Some(before) = before {
        capacity.index_pages = checked_exact_sub(
            "Virtual Wait activation index pages",
            capacity.index_pages,
            1,
        )?;
        capacity.source_bytes = checked_exact_sub(
            "Virtual Wait activation source bytes",
            capacity.source_bytes,
            virtual_index_page_source_bytes(before)?,
        )?;
        capacity.mutation_bytes = checked_exact_sub(
            "Virtual Wait activation mutation bytes",
            capacity.mutation_bytes,
            virtual_index_page_delete_bytes(before)?,
        )?;
    }
    if let Some(after) = after {
        capacity.index_pages = checked_exact_add(
            "Virtual Wait activation index pages",
            capacity.index_pages,
            1,
        )?;
        capacity.source_bytes = checked_exact_add(
            "Virtual Wait activation source bytes",
            capacity.source_bytes,
            virtual_index_page_source_bytes(after)?,
        )?;
        capacity.mutation_bytes = checked_exact_add(
            "Virtual Wait activation mutation bytes",
            capacity.mutation_bytes,
            virtual_index_page_delete_bytes(after)?,
        )?;
    }
    Ok(())
}

fn add_virtual_wait_work_capacity(
    capacity: &mut VirtualWaitActivationCapacity,
    after_work: &VirtualWorkCurrent,
    parked: &VirtualParkedCurrent,
) -> ProtocolResult<()> {
    capacity.work_items =
        checked_exact_add("Virtual Wait activation work count", capacity.work_items, 1)?;
    for leaf in [
        VirtualStateLeaf::Parked(parked.clone()),
        VirtualStateLeaf::Work(after_work.clone()),
    ] {
        capacity.source_bytes = checked_exact_add(
            "Virtual Wait activation source bytes",
            capacity.source_bytes,
            virtual_activation_source_leaf_bytes(&leaf)?,
        )?;
    }
    let mut ready_work = after_work.clone();
    ready_work.placement = VirtualWorkPlacement::Ready;
    for mutation in [
        VirtualStateMutation::Parked {
            before: Some(parked.clone()),
            after: None,
        },
        VirtualStateMutation::Work {
            before: Some(after_work.clone()),
            after: Some(ready_work),
        },
    ] {
        capacity.mutation_bytes = checked_exact_add(
            "Virtual Wait activation mutation bytes",
            capacity.mutation_bytes,
            virtual_activation_mutation_bytes(&mutation)?,
        )?;
    }
    Ok(())
}

fn reduce_virtual_migration(
    persistence: &VirtualMigrationPersistenceCommand,
    source: &VirtualKeyedSource,
    command: RegionMigrationCommand,
    coverage_evidence: ArtifactRecord,
    target_source_artifacts: Vec<ArtifactRecord>,
) -> ProtocolResult<ReducedVirtualOperation> {
    let (current, source_ids, target_ids) = validate_virtual_migration_source(
        persistence,
        source,
        &command,
        &coverage_evidence,
        &target_source_artifacts,
    )?;
    let plan = &command.plan;
    let (run_id, source_operation, mut mutations) = retire_virtual_migration_sources(plan, source)?;
    append_virtual_migration_targets(
        persistence,
        plan,
        &run_id,
        &source_operation,
        &mut mutations,
    )?;
    let mut frontier = current.body.frontier.clone();
    if frontier
        .last_region
        .as_ref()
        .is_some_and(|region_id| source_ids.contains(region_id))
    {
        frontier.last_region = None;
    }
    let receipt = RegionMigrationReceipt {
        plan: plan.clone(),
        retired_regions: source_ids,
        active_targets: target_ids,
    };
    mutations.push(VirtualStateMutation::Migrations {
        before: None,
        after: Some(VirtualMigrationCurrent {
            leaf_version: VIRTUAL_MIGRATION_CURRENT_VERSION.to_owned(),
            scheduler_id: persistence.scheduler_id.clone(),
            receipt: receipt.clone(),
        }),
    });
    let mut counts = current.body.counts;
    counts.regions = checked_exact_add(
        "Virtual region count",
        counts.regions,
        plan.targets.len() as u64,
    )?;
    counts.migrations = checked_exact_add("Virtual migration count", counts.migrations, 1)?;
    counts.active_regions = checked_exact_sub(
        "Virtual active region count",
        counts.active_regions,
        plan.expected_sources.len() as u64,
    )?;
    counts.active_regions = checked_exact_add(
        "Virtual active region count",
        counts.active_regions,
        plan.targets.len() as u64,
    )?;
    let mut artifacts = vec![coverage_evidence.clone()];
    artifacts.extend(target_source_artifacts.iter().cloned());
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::Migrated {
            command,
            coverage_evidence,
            target_source_artifacts,
        },
        current: draft_from_current(current, frontier, counts),
        mutations,
        outcome: VirtualPersistenceOutcome::Migrated(receipt),
        artifacts,
        archive_pin: None,
        archive_release: None,
    })
}

fn validate_virtual_migration_source<'a>(
    persistence: &VirtualMigrationPersistenceCommand,
    source: &'a VirtualKeyedSource,
    command: &RegionMigrationCommand,
    coverage_evidence: &ArtifactRecord,
    target_source_artifacts: &[ArtifactRecord],
) -> ProtocolResult<(&'a VirtualCurrent, BTreeSet<String>, BTreeSet<String>)> {
    let current = require_virtual_current(source)?;
    require_only_source_families(
        source,
        &[
            VirtualStateFamily::Regions,
            VirtualStateFamily::ActiveRegions,
            VirtualStateFamily::Migrations,
        ],
    )?;
    verify_migration_evidence(
        persistence,
        command,
        coverage_evidence,
        target_source_artifacts,
    )?;
    let source_ids = command
        .plan
        .expected_sources
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let target_ids = command
        .plan
        .targets
        .iter()
        .map(|target| target.region_id.clone())
        .collect::<BTreeSet<_>>();
    if !source.migrations.is_empty()
        || source.regions.keys().cloned().collect::<BTreeSet<_>>() != source_ids
        || source
            .active_regions
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != source_ids
        || !source_ids.is_disjoint(&target_ids)
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual migration source is missing an exact source or found a target/migration collision"
                .to_owned(),
        ));
    }
    Ok((current, source_ids, target_ids))
}

fn retire_virtual_migration_sources(
    plan: &RegionMigrationPlan,
    source: &VirtualKeyedSource,
) -> ProtocolResult<(String, String, Vec<VirtualStateMutation>)> {
    let mut run_id = None::<String>;
    let mut source_operation = None::<String>;
    let mut mutations = Vec::new();
    for (region_id, expected) in &plan.expected_sources {
        let before = source.regions[region_id].clone();
        let active_before = source.active_regions[region_id].clone();
        if before.lifecycle != VirtualRegionLifecycle::Active
            || before.region.source != expected.source
            || before.region.cursor != expected.cursor
            || before.region.cursor.exhausted
            || before.compaction_certificate_id.is_some()
        {
            return Err(ProtocolError::IllegalTransition(format!(
                "Virtual migration source {region_id} is retired, compacted, or stale"
            )));
        }
        if run_id
            .as_ref()
            .is_some_and(|expected| expected != &before.region.run_id)
            || source_operation
                .as_ref()
                .is_some_and(|expected| expected != &before.region.source.operation)
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual migration sources span Run or source-operation authorities".to_owned(),
            ));
        }
        run_id.get_or_insert_with(|| before.region.run_id.clone());
        source_operation.get_or_insert_with(|| before.region.source.operation.clone());
        let mut after = before.clone();
        after.lifecycle = VirtualRegionLifecycle::Retired {
            migration_id: plan.migration_id.clone(),
        };
        mutations.push(VirtualStateMutation::Regions {
            before: Some(before),
            after: Some(after),
        });
        mutations.push(VirtualStateMutation::ActiveRegions {
            before: Some(active_before),
            after: None,
        });
    }
    Ok((
        run_id.ok_or_else(|| {
            ProtocolError::Validation("Virtual migration has no source Run".to_owned())
        })?,
        source_operation.ok_or_else(|| {
            ProtocolError::Validation("Virtual migration has no source operation".to_owned())
        })?,
        mutations,
    ))
}

fn append_virtual_migration_targets(
    persistence: &VirtualMigrationPersistenceCommand,
    plan: &RegionMigrationPlan,
    run_id: &str,
    source_operation: &str,
    mutations: &mut Vec<VirtualStateMutation>,
) -> ProtocolResult<()> {
    for target in &plan.targets {
        if target.run_id != run_id || target.source.operation != source_operation {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual migration target changed Run or source-operation authority".to_owned(),
            ));
        }
        if target.cursor.exhausted {
            return Err(ProtocolError::IllegalTransition(
                "Virtual migration target cannot enter the active ordering already exhausted"
                    .to_owned(),
            ));
        }
        mutations.push(VirtualStateMutation::Regions {
            before: None,
            after: Some(VirtualRegionCurrent {
                leaf_version: VIRTUAL_REGION_CURRENT_VERSION.to_owned(),
                scheduler_id: persistence.scheduler_id.clone(),
                region: target.clone(),
                lifecycle: VirtualRegionLifecycle::Active,
                hot_work_count: 0,
                hot_occurrence_count: 0,
                compaction_certificate_id: None,
            }),
        });
        mutations.push(VirtualStateMutation::ActiveRegions {
            before: None,
            after: Some(VirtualActiveRegionCurrent {
                leaf_version: VIRTUAL_ACTIVE_REGION_CURRENT_VERSION.to_owned(),
                scheduler_id: persistence.scheduler_id.clone(),
                region_id: target.region_id.clone(),
            }),
        });
    }
    Ok(())
}

fn reduce_virtual_lease_renewal(
    persistence: &VirtualLeaseRenewalPersistenceCommand,
    source: &VirtualKeyedSource,
    clock: ClockObservation,
    lease: VirtualClaimLease,
) -> ProtocolResult<ReducedVirtualOperation> {
    let current = require_virtual_current(source)?;
    require_only_source_families(
        source,
        &[VirtualStateFamily::Work, VirtualStateFamily::Occurrences],
    )?;
    let command = &persistence.command;
    let claim = current
        .body
        .frontier
        .active
        .get(&command.work_id)
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(format!(
                "Virtual active work {} is missing",
                command.work_id
            ))
        })?;
    verify_clock_observation(&command.clock, &clock, &claim.lease.resource)?;
    verify_clock_timeline(&claim.lease.clock, &command.clock)?;
    verify_claim_lease(&lease)?;
    let expected_lease_epoch = checked_exact_add(
        "Virtual capacity-slot lease epoch",
        command.expected_lease_epoch,
        1,
    )?;
    let expected_expiry = virtual_lease_expiry(clock.logical_time, command.lease_ttl)?;
    if claim.owner != command.owner
        || claim.epoch != command.epoch
        || claim.lease.epoch != command.expected_lease_epoch
        || clock.logical_time >= claim.lease.expires_at
        || expected_expiry <= claim.lease.expires_at
        || lease.resource != claim.lease.resource
        || lease.owner != claim.owner
        || lease.epoch != expected_lease_epoch
        || lease.expires_at != expected_expiry
        || lease.clock != command.clock
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual lease renewal is stale or does not equal the Durable-derived next fence"
                .to_owned(),
        ));
    }
    let occurrence_id = claim.occurrence_id.clone();
    if source.work.keys().cloned().collect::<BTreeSet<_>>()
        != BTreeSet::from([command.work_id.clone()])
        || source.occurrences.keys().cloned().collect::<BTreeSet<_>>()
            != BTreeSet::from([occurrence_id.clone()])
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual renewal did not load exactly its work and occurrence leaves".to_owned(),
        ));
    }
    let work = &source.work[&command.work_id];
    let before = source.occurrences[&occurrence_id].clone();
    if work.placement != VirtualWorkPlacement::Active
        || work.item != claim.item
        || work.max_epoch != claim.epoch
        || work.latest_occurrence_id.as_deref() != Some(occurrence_id.as_str())
        || before.occurrence.state != WorkOccurrenceState::Running
        || before.occurrence.lease_epoch != command.expected_lease_epoch
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual renewal frontier and exact work/occurrence leaves disagree".to_owned(),
        ));
    }
    let mut after = before.clone();
    after.occurrence.lease_epoch = lease.epoch;
    after.occurrence.lease_clock = lease.clock.clone();
    let mut frontier = current.body.frontier.clone();
    let mut renewed = claim.clone();
    renewed.lease = lease.clone();
    frontier.active.insert(command.work_id.clone(), renewed);
    let receipt = VirtualLeaseRenewalReceipt {
        command: command.clone(),
        clock_observation: clock,
        lease,
    };
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::None,
        current: draft_from_current(current, frontier, current.body.counts),
        mutations: vec![VirtualStateMutation::Occurrences {
            before: Some(Box::new(before)),
            after: Some(Box::new(after)),
        }],
        outcome: VirtualPersistenceOutcome::LeaseRenewed(receipt),
        artifacts: Vec::new(),
        archive_pin: None,
        archive_release: None,
    })
}

fn reduce_virtual_run_weight(
    persistence: &VirtualRunWeightPersistenceCommand,
    source: &VirtualKeyedSource,
) -> ProtocolResult<ReducedVirtualOperation> {
    let current = require_virtual_current(source)?;
    require_only_source_families(source, &[VirtualStateFamily::Runs])?;
    let command = &persistence.command;
    if source.runs.keys().cloned().collect::<BTreeSet<_>>()
        != BTreeSet::from([command.run_id.clone()])
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual Run-weight update did not load its exact Run leaf".to_owned(),
        ));
    }
    let before = source.runs[&command.run_id].clone();
    let previous_weight = before.weight;
    let mut after = before.clone();
    after.weight = command.weight;
    after.deficit = 0;
    let mutations = (after != before)
        .then_some(VirtualStateMutation::Runs {
            before: Some(before),
            after: Some(after),
        })
        .into_iter()
        .collect();
    let receipt = VirtualRunWeightReceipt {
        command: command.clone(),
        previous_weight,
        current_weight: command.weight,
    };
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::None,
        current: draft_from_current(current, current.body.frontier.clone(), current.body.counts),
        mutations,
        outcome: VirtualPersistenceOutcome::RunWeightSet(receipt),
        artifacts: Vec::new(),
        archive_pin: None,
        archive_release: None,
    })
}

fn verify_clock_observation(
    reference: &ClockObservationRef,
    observation: &ClockObservation,
    expected_scope: &str,
) -> ProtocolResult<()> {
    reference.verify()?;
    observation.verify()?;
    if observation.reference() != *reference || observation.scope != expected_scope {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual Clock observation does not equal its exact command reference and scope"
                .to_owned(),
        ));
    }
    Ok(())
}

fn verify_clock_timeline(
    expected: &ClockObservationRef,
    proposed: &ClockObservationRef,
) -> ProtocolResult<()> {
    expected.verify()?;
    proposed.verify()?;
    if expected.source_id != proposed.source_id
        || expected.source_generation != proposed.source_generation
        || expected.scope != proposed.scope
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual lease Clock source, generation, or scope changed".to_owned(),
        ));
    }
    Ok(())
}

fn verify_claim_lease(lease: &VirtualClaimLease) -> ProtocolResult<()> {
    validate_identity("Virtual claim lease resource", &lease.resource)?;
    validate_identity("Virtual claim lease owner", &lease.owner)?;
    validate_positive_exact("Virtual claim lease epoch", lease.epoch)?;
    validate_exact("Virtual claim lease expiry", lease.expires_at)?;
    lease.clock.verify()?;
    if lease.clock.scope != lease.resource {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual claim lease Clock scope changed its capacity slot".to_owned(),
        ));
    }
    Ok(())
}

fn verify_claimed_work(claim: &ClaimedWork) -> ProtocolResult<()> {
    validate_work_item(&claim.item)?;
    validate_identity("Virtual claim owner", &claim.owner)?;
    validate_identity("Virtual claim occurrence", &claim.occurrence_id)?;
    validate_content_id("Virtual claim Plan", &claim.plan_id)?;
    validate_execution_binding(&claim.execution_binding)?;
    validate_positive_exact("Virtual claim epoch", claim.epoch)?;
    verify_claim_lease(&claim.lease)?;
    if claim.owner != claim.lease.owner
        || claim.occurrence_id
            != cymule_core::content_id(
                VIRTUAL_WORK_OCCURRENCE_VERSION,
                &(claim.item.work_id.as_str(), claim.epoch),
            )?
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual claim changed its owner, lease, or occurrence identity".to_owned(),
        ));
    }
    Ok(())
}

fn verify_virtual_claim_receipt(
    virtual_persistence_id: &str,
    receipt: &VirtualClaimReceipt,
) -> ProtocolResult<()> {
    receipt.command.verify()?;
    verify_clock_observation(
        &receipt.command.clock,
        &receipt.clock_observation,
        &receipt.command.slot_id,
    )?;
    verify_virtual_evolution_selection(
        virtual_persistence_id,
        receipt.run_execution.as_ref(),
        receipt.claim.as_ref(),
        receipt.evolution_selection.as_ref(),
    )?;
    if let Some(claim) = &receipt.claim {
        verify_claimed_work(claim)?;
        let expected_expiry = virtual_lease_expiry(
            receipt.clock_observation.logical_time,
            receipt.command.lease_ttl,
        )?;
        if claim.owner != receipt.command.owner
            || claim.execution_binding != receipt.command.execution_binding
            || claim.lease.resource != receipt.command.slot_id
            || claim.lease.clock != receipt.command.clock
            || claim.lease.expires_at != expected_expiry
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual claim receipt changed its command, Clock, lease, or ExecutionBinding"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn virtual_lease_expiry(logical_now: u64, ttl: u64) -> ProtocolResult<u64> {
    validate_exact("Virtual Clock logical time", logical_now)?;
    validate_positive_exact("Virtual lease TTL", ttl)?;
    checked_exact_add("Virtual lease expiry", logical_now, ttl)
}

#[derive(Clone)]
struct VirtualRunCandidate {
    run_id: String,
    item_index: usize,
    cost: u64,
}

/// Preview the exact bounded fairness choice before Durable loads the selected
/// Plan/binding and optionally reduces an Evolution selection.
///
/// The source must contain only the scalar current and every ready Run leaf.
/// Durable may then exact-load the returned work and region leaves from the
/// same pinned revision before calling [`prepare_virtual`].
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn preview_virtual_claim(
    persistence: &VirtualClaimPersistenceCommand,
    source: &VirtualKeyedSource,
) -> VirtualPreparationResult<Option<VirtualClaimPreview>> {
    persistence.command.verify()?;
    let current = require_virtual_current(source)?;
    let mut required = BTreeSet::new();
    if current.body.frontier.active.len() < current.body.limits.max_active {
        for run_id in current.body.frontier.ready.keys() {
            require_virtual_local_read(source, &mut required, VirtualStateFamily::Runs, run_id)?;
        }
    }
    require_only_source_families(source, &[VirtualStateFamily::Runs])?;
    if source.lookups != required {
        return Err(ProtocolError::IllegalTransition(
            "Virtual claim preview source contains an orphan exact family read".to_owned(),
        )
        .into());
    }
    preview_virtual_claim_loaded(persistence, source).map_err(Into::into)
}

fn preview_virtual_claim_loaded(
    persistence: &VirtualClaimPersistenceCommand,
    source: &VirtualKeyedSource,
) -> ProtocolResult<Option<VirtualClaimPreview>> {
    persistence.command.verify()?;
    let current = require_virtual_current(source)?;
    if current.body.scheduler_id != persistence.scheduler_id {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual claim preview targets a different scheduler current".to_owned(),
        ));
    }
    // Final command preparation reuses this selection after loading the exact
    // work and region leaves. Only the public preview requires a Runs-only view.
    let globally_full = current.body.frontier.active.len() >= current.body.limits.max_active;
    let ready_runs = current
        .body
        .frontier
        .ready
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if globally_full {
        if !source.runs.is_empty() {
            return Err(ProtocolError::IllegalTransition(
                "capacity-full Virtual claim preview loaded orphan Run leaves".to_owned(),
            ));
        }
        return Ok(None);
    }
    if source.runs.keys().cloned().collect::<BTreeSet<_>>() != ready_runs {
        return Err(ProtocolError::IllegalTransition(
            "Virtual claim preview did not load exactly every bounded ready Run leaf".to_owned(),
        ));
    }
    let candidates = virtual_claim_candidates(current, source, &persistence.command.capabilities)?;
    let Some((selected, _)) = virtual_select_claim(current, source, &candidates)? else {
        return Ok(None);
    };
    let item = current
        .body
        .frontier
        .ready
        .get(&selected.run_id)
        .and_then(|queue| queue.get(selected.item_index))
        .cloned()
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(
                "Virtual claim preview selected work outside the bounded ready frontier".to_owned(),
            )
        })?;
    let execution = source.runs[&selected.run_id].execution.clone();
    Ok(Some(VirtualClaimPreview { item, execution }))
}

fn reduce_virtual_claim(
    persistence: &VirtualClaimPersistenceCommand,
    virtual_persistence_id: &str,
    source: &VirtualKeyedSource,
    clock: ClockObservation,
    lease: &VirtualClaimLease,
    execution: &VirtualExecutionAuthority,
    evolution_selection: Option<VirtualEvolutionSelectionLink>,
) -> ProtocolResult<ReducedVirtualOperation> {
    let selection =
        validate_virtual_claim_selection(persistence, source, &clock, lease, execution)?;
    let mut transition = VirtualClaimTransition {
        frontier: selection.current.body.frontier.clone(),
        mutations: Vec::new(),
        counts: selection.current.body.counts,
    };
    let (claim, claimed_run_execution) = if selection.selected.is_some() {
        let applied = apply_selected_virtual_claim(
            persistence,
            source,
            lease,
            execution,
            &selection,
            &mut transition,
        )?;
        (Some(applied.0), Some(applied.1))
    } else {
        if execution.selected.is_some()
            || evolution_selection.is_some()
            || !source.work.is_empty()
            || !source.occurrences.is_empty()
            || !source.regions.is_empty()
        {
            return Err(ProtocolError::IllegalTransition(
                "empty Virtual claim received eligible-work-only authority".to_owned(),
            ));
        }
        (None, None)
    };
    let receipt = VirtualClaimReceipt {
        command: persistence.command.clone(),
        clock_observation: clock,
        claim,
        run_execution: claimed_run_execution,
        evolution_selection,
    };
    verify_virtual_evolution_selection(
        virtual_persistence_id,
        receipt.run_execution.as_ref(),
        receipt.claim.as_ref(),
        receipt.evolution_selection.as_ref(),
    )?;
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::None,
        current: draft_from_current(selection.current, transition.frontier, transition.counts),
        mutations: transition.mutations,
        outcome: VirtualPersistenceOutcome::Claimed(receipt),
        artifacts: Vec::new(),
        archive_pin: None,
        archive_release: None,
    })
}

struct VirtualClaimSelection<'a> {
    current: &'a VirtualCurrent,
    candidates: Vec<VirtualRunCandidate>,
    selected: Option<(VirtualRunCandidate, u64)>,
}

struct VirtualClaimTransition {
    frontier: VirtualFrontierCurrent,
    mutations: Vec<VirtualStateMutation>,
    counts: VirtualCurrentCounts,
}

struct ValidatedSelectedClaim {
    item: WorkItem,
    execution: VirtualSelectedExecution,
    before_work: VirtualWorkCurrent,
    before_region: VirtualRegionCurrent,
    next_deficits: BTreeMap<String, u64>,
}

fn validate_virtual_claim_selection<'a>(
    persistence: &VirtualClaimPersistenceCommand,
    source: &'a VirtualKeyedSource,
    clock: &ClockObservation,
    lease: &VirtualClaimLease,
    execution: &VirtualExecutionAuthority,
) -> ProtocolResult<VirtualClaimSelection<'a>> {
    let current = require_virtual_current(source)?;
    require_only_source_families(
        source,
        &[
            VirtualStateFamily::Regions,
            VirtualStateFamily::Work,
            VirtualStateFamily::Occurrences,
            VirtualStateFamily::Runs,
        ],
    )?;
    let command = &persistence.command;
    execution.verify()?;
    if execution.execution_binding.reference != command.execution_binding {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual claim resolved a different ExecutionBinding record".to_owned(),
        ));
    }
    verify_clock_observation(&command.clock, clock, &command.slot_id)?;
    verify_claim_lease(lease)?;
    let expected_expiry = virtual_lease_expiry(clock.logical_time, command.lease_ttl)?;
    if lease.resource != command.slot_id
        || lease.owner != command.owner
        || lease.expires_at != expected_expiry
        || lease.clock != command.clock
        || current
            .body
            .frontier
            .active
            .values()
            .any(|claim| claim.lease.resource == lease.resource)
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual claim lease is not the exact free slot fence derived by Durable".to_owned(),
        ));
    }
    let globally_full = current.body.frontier.active.len() >= current.body.limits.max_active;
    let ready_runs = current
        .body
        .frontier
        .ready
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if globally_full && !source.runs.is_empty() {
        return Err(ProtocolError::IllegalTransition(
            "capacity-full Virtual claim loaded orphan Run leaves".to_owned(),
        ));
    }
    if !globally_full && source.runs.keys().cloned().collect::<BTreeSet<_>>() != ready_runs {
        return Err(ProtocolError::IllegalTransition(
            "Virtual claim did not load exactly every bounded ready Run leaf".to_owned(),
        ));
    }
    let candidates = if globally_full {
        Vec::new()
    } else {
        virtual_claim_candidates(current, source, &command.capabilities)?
    };
    let selected = virtual_select_claim(current, source, &candidates)?;
    Ok(VirtualClaimSelection {
        current,
        candidates,
        selected,
    })
}

fn validate_selected_virtual_claim(
    source: &VirtualKeyedSource,
    execution: &VirtualExecutionAuthority,
    selection: &VirtualClaimSelection<'_>,
) -> ProtocolResult<ValidatedSelectedClaim> {
    let (selected, rounds) = selection.selected.as_ref().ok_or_else(|| {
        ProtocolError::IllegalTransition("Virtual claim has no fairness selection".to_owned())
    })?;
    let item = selection
        .current
        .body
        .frontier
        .ready
        .get(&selected.run_id)
        .and_then(|queue| queue.get(selected.item_index))
        .cloned()
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(
                "Virtual fairness selected work outside the bounded ready frontier".to_owned(),
            )
        })?;
    if source.work.keys().cloned().collect::<BTreeSet<_>>()
        != BTreeSet::from([item.work_id.clone()])
        || source.regions.keys().cloned().collect::<BTreeSet<_>>()
            != BTreeSet::from([item.region_id.clone()])
        || !source.occurrences.is_empty()
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual claim did not load the exact selected work/region or found an occurrence collision"
                .to_owned(),
        ));
    }
    let selected_execution = execution.selected.as_ref().ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "eligible Virtual work requires exact current execution authority".to_owned(),
        )
    })?;
    if selected_execution.run_id != item.run_id || selected_execution.plan.plan_id.is_empty() {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual claim execution authority changed Run, Plan, or ExecutionBinding".to_owned(),
        ));
    }
    let before_work = source.work[&item.work_id].clone();
    let before_region = source.regions[&item.region_id].clone();
    if before_work.item != item
        || before_work.placement != VirtualWorkPlacement::Ready
        || before_region.region.region_id != item.region_id
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual selected frontier work disagrees with its exact keyed leaves".to_owned(),
        ));
    }
    let next_deficits = virtual_claim_deficits(
        selection.current,
        source,
        &selection.candidates,
        selected,
        *rounds,
    )?;
    Ok(ValidatedSelectedClaim {
        item,
        execution: selected_execution.clone(),
        before_work,
        before_region,
        next_deficits,
    })
}

fn apply_selected_virtual_claim(
    persistence: &VirtualClaimPersistenceCommand,
    source: &VirtualKeyedSource,
    lease: &VirtualClaimLease,
    execution: &VirtualExecutionAuthority,
    selection: &VirtualClaimSelection<'_>,
    transition: &mut VirtualClaimTransition,
) -> ProtocolResult<(ClaimedWork, VirtualRunExecution)> {
    let validated = validate_selected_virtual_claim(source, execution, selection)?;
    for (run_id, &deficit) in &validated.next_deficits {
        let before = source.runs[run_id].clone();
        if before.deficit != deficit {
            let mut after = before.clone();
            after.deficit = deficit;
            transition.mutations.push(VirtualStateMutation::Runs {
                before: Some(before),
                after: Some(after),
            });
        }
    }
    remove_ready_frontier(
        &mut transition.frontier,
        &validated.item.run_id,
        &validated.item.work_id,
    )?;
    transition.frontier.dispatch_sequence = checked_exact_add(
        "Virtual dispatch sequence",
        transition.frontier.dispatch_sequence,
        1,
    )?;
    transition.frontier.last_run = Some(validated.item.run_id.clone());
    let epoch = checked_exact_add(
        "Virtual work claim epoch",
        validated.before_work.max_epoch,
        1,
    )?;
    let occurrence_id = cymule_core::content_id(
        VIRTUAL_WORK_OCCURRENCE_VERSION,
        &(validated.item.work_id.as_str(), epoch),
    )?;
    let claim = ClaimedWork {
        item: validated.item.clone(),
        owner: persistence.command.owner.clone(),
        epoch,
        occurrence_id: occurrence_id.clone(),
        plan_id: validated.execution.plan.plan_id.clone(),
        execution_binding: execution.execution_binding.reference.clone(),
        lease: lease.clone(),
    };
    append_virtual_claim_records(
        persistence,
        execution,
        lease,
        &validated,
        &claim,
        &occurrence_id,
        transition,
    )?;
    transition
        .frontier
        .active
        .insert(validated.item.work_id.clone(), claim.clone());
    transition.counts.hot_occurrences = checked_exact_add(
        "Virtual hot occurrence count",
        transition.counts.hot_occurrences,
        1,
    )?;
    Ok((claim, source.runs[&validated.item.run_id].execution.clone()))
}

fn append_virtual_claim_records(
    persistence: &VirtualClaimPersistenceCommand,
    execution: &VirtualExecutionAuthority,
    lease: &VirtualClaimLease,
    validated: &ValidatedSelectedClaim,
    claim: &ClaimedWork,
    occurrence_id: &str,
    transition: &mut VirtualClaimTransition,
) -> ProtocolResult<()> {
    let occurrence = WorkOccurrence {
        occurrence_version: VIRTUAL_WORK_OCCURRENCE_VERSION.to_owned(),
        occurrence_id: occurrence_id.to_owned(),
        work_id: validated.item.work_id.clone(),
        region_id: validated.item.region_id.clone(),
        run_id: validated.item.run_id.clone(),
        owner: persistence.command.owner.clone(),
        epoch: claim.epoch,
        lease_epoch: lease.epoch,
        lease_clock: lease.clock.clone(),
        plan_id: validated.execution.plan.plan_id.clone(),
        execution_binding: execution.execution_binding.reference.clone(),
        state: WorkOccurrenceState::Running,
        result: None,
        error: None,
        next_reason: None,
    };
    let mut after_work = validated.before_work.clone();
    after_work.max_epoch = claim.epoch;
    after_work.latest_occurrence_id = Some(occurrence_id.to_owned());
    after_work.placement = VirtualWorkPlacement::Active;
    let mut after_region = validated.before_region.clone();
    after_region.hot_occurrence_count = checked_exact_add(
        "Virtual region hot occurrence count",
        after_region.hot_occurrence_count,
        1,
    )?;
    transition.mutations.push(VirtualStateMutation::Work {
        before: Some(validated.before_work.clone()),
        after: Some(after_work),
    });
    transition
        .mutations
        .push(VirtualStateMutation::Occurrences {
            before: None,
            after: Some(Box::new(VirtualOccurrenceCurrent {
                leaf_version: VIRTUAL_OCCURRENCE_CURRENT_VERSION.to_owned(),
                scheduler_id: persistence.scheduler_id.clone(),
                occurrence,
            })),
        });
    transition.mutations.push(VirtualStateMutation::Regions {
        before: Some(validated.before_region.clone()),
        after: Some(after_region),
    });
    Ok(())
}

fn virtual_claim_candidates(
    current: &VirtualCurrent,
    source: &VirtualKeyedSource,
    capabilities: &BTreeSet<String>,
) -> ProtocolResult<Vec<VirtualRunCandidate>> {
    let mut runs = current
        .body
        .frontier
        .ready
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    if let Some(last) = &current.body.frontier.last_run {
        let index = runs.iter().position(|run_id| run_id > last).unwrap_or(0);
        runs.rotate_left(index);
    }
    let mut candidates = Vec::new();
    for run_id in runs {
        let run = source.runs.get(&run_id).ok_or_else(|| {
            ProtocolError::IllegalTransition(format!(
                "Virtual ready Run {run_id} has no exact fairness leaf"
            ))
        })?;
        if run.scheduler_id != current.body.scheduler_id {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual fairness leaf changed scheduler ownership".to_owned(),
            ));
        }
        let active = current
            .body
            .frontier
            .active
            .values()
            .filter(|claim| claim.item.run_id == run_id)
            .count();
        if active >= current.body.limits.max_active_per_run {
            continue;
        }
        let queue = &current.body.frontier.ready[&run_id];
        let mut best = None::<(usize, i128)>;
        for (index, item) in queue.iter().enumerate() {
            if item
                .capability
                .as_ref()
                .is_some_and(|capability| !capabilities.contains(capability))
            {
                continue;
            }
            let since = current.body.frontier.ready_since[&item.work_id];
            let age = current
                .body
                .frontier
                .dispatch_sequence
                .checked_sub(since)
                .ok_or_else(|| {
                    ProtocolError::IllegalTransition(
                        "Virtual ready age exceeds the dispatch head".to_owned(),
                    )
                })?
                / current.body.scheduling_policy.aging_interval;
            let score = i128::from(item.priority) + i128::from(age);
            if best.is_none_or(|(_, current)| score > current) {
                best = Some((index, score));
            }
        }
        if let Some((item_index, _)) = best {
            candidates.push(VirtualRunCandidate {
                run_id,
                item_index,
                cost: queue[item_index].cost,
            });
        }
    }
    Ok(candidates)
}

fn virtual_select_claim(
    current: &VirtualCurrent,
    source: &VirtualKeyedSource,
    candidates: &[VirtualRunCandidate],
) -> ProtocolResult<Option<(VirtualRunCandidate, u64)>> {
    let Some(rounds) = candidates
        .iter()
        .map(|candidate| {
            let run = &source.runs[&candidate.run_id];
            let quantum = virtual_run_quantum(current, run)?;
            Ok(required_virtual_deficit_rounds(
                run.deficit,
                candidate.cost,
                quantum,
            ))
        })
        .collect::<ProtocolResult<Vec<_>>>()?
        .into_iter()
        .min()
    else {
        return Ok(None);
    };
    let selected = candidates
        .iter()
        .find(|candidate| {
            let run = &source.runs[&candidate.run_id];
            virtual_run_quantum(current, run).is_ok_and(|quantum| {
                required_virtual_deficit_rounds(run.deficit, candidate.cost, quantum) <= rounds
            })
        })
        .cloned()
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(
                "Virtual fairness found no candidate at its minimum deficit round".to_owned(),
            )
        })?;
    Ok(Some((selected, rounds)))
}

fn virtual_claim_deficits(
    current: &VirtualCurrent,
    source: &VirtualKeyedSource,
    candidates: &[VirtualRunCandidate],
    selected: &VirtualRunCandidate,
    rounds: u64,
) -> ProtocolResult<BTreeMap<String, u64>> {
    let selected_index = candidates
        .iter()
        .position(|candidate| {
            candidate.run_id == selected.run_id && candidate.item_index == selected.item_index
        })
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(
                "Virtual selected Run is absent from its fairness candidates".to_owned(),
            )
        })?;
    let rounds_before_final = if rounds == 0 { 0 } else { rounds - 1 };
    let mut result = BTreeMap::new();
    for (index, candidate) in candidates.iter().enumerate() {
        let run = &source.runs[&candidate.run_id];
        let quantum = virtual_run_quantum(current, run)?;
        let deficit = if index == selected_index {
            virtual_deficit_after_claim(run.deficit, candidate.cost, quantum, rounds)?
        } else {
            let credited = if index < selected_index {
                rounds
            } else {
                rounds_before_final
            };
            virtual_deficit_after_incomplete_rounds(run.deficit, candidate.cost, quantum, credited)?
        };
        result.insert(candidate.run_id.clone(), deficit);
    }
    Ok(result)
}

fn virtual_run_quantum(current: &VirtualCurrent, run: &VirtualRunCurrent) -> ProtocolResult<u64> {
    current
        .body
        .scheduling_policy
        .base_quantum
        .checked_mul(u64::from(run.weight))
        .filter(|value| *value <= cymule_core::MAX_EXACT_INTEGER)
        .ok_or_else(|| {
            ProtocolError::Validation("Virtual Run quantum exceeds exact range".to_owned())
        })
}

fn required_virtual_deficit_rounds(deficit: u64, cost: u64, quantum: u64) -> u64 {
    if deficit >= cost {
        return 0;
    }
    let missing = cost - deficit;
    missing / quantum + u64::from(!missing.is_multiple_of(quantum))
}

fn virtual_deficit_after_incomplete_rounds(
    deficit: u64,
    cost: u64,
    quantum: u64,
    rounds: u64,
) -> ProtocolResult<u64> {
    if rounds == 0 {
        return Ok(deficit);
    }
    if rounds >= required_virtual_deficit_rounds(deficit, cost, quantum) {
        return Err(ProtocolError::IllegalTransition(
            "Virtual fairness advanced an unselected Run through a payable round".to_owned(),
        ));
    }
    let grant = quantum
        .checked_mul(rounds)
        .filter(|value| *value <= cymule_core::MAX_EXACT_INTEGER)
        .ok_or_else(|| {
            ProtocolError::Validation("Virtual deficit grant exceeds exact range".to_owned())
        })?;
    let next = checked_exact_add("Virtual incomplete-round deficit", deficit, grant)?;
    if next >= cost {
        return Err(ProtocolError::IllegalTransition(
            "Virtual unselected Run retained a payable deficit".to_owned(),
        ));
    }
    Ok(next)
}

fn virtual_deficit_after_claim(
    deficit: u64,
    cost: u64,
    quantum: u64,
    rounds: u64,
) -> ProtocolResult<u64> {
    if rounds != required_virtual_deficit_rounds(deficit, cost, quantum) {
        return Err(ProtocolError::IllegalTransition(
            "Virtual fairness selected a Run at the wrong deficit round".to_owned(),
        ));
    }
    if rounds == 0 {
        return deficit.checked_sub(cost).ok_or_else(|| {
            ProtocolError::IllegalTransition(
                "Virtual fairness selected a Run without sufficient deficit".to_owned(),
            )
        });
    }
    let before_final = virtual_deficit_after_incomplete_rounds(deficit, cost, quantum, rounds - 1)?;
    let missing = cost.checked_sub(before_final).ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Virtual fairness final round began with a payable deficit".to_owned(),
        )
    })?;
    if missing == 0 || missing > quantum {
        return Err(ProtocolError::IllegalTransition(
            "Virtual fairness final quantum cannot settle selected cost".to_owned(),
        ));
    }
    Ok(quantum - missing)
}

fn verify_virtual_evolution_selection(
    virtual_persistence_id: &str,
    execution: Option<&VirtualRunExecution>,
    claim: Option<&ClaimedWork>,
    link: Option<&VirtualEvolutionSelectionLink>,
) -> ProtocolResult<()> {
    match (execution, claim, link) {
        (None, None, None) => Ok(()),
        (Some(VirtualRunExecution::Direct { plan_id }), Some(claim), None)
            if plan_id == &claim.plan_id =>
        {
            Ok(())
        }
        (
            Some(VirtualRunExecution::Evolution {
                evolution_id,
                template_id,
            }),
            Some(claim),
            Some(link),
        ) => {
            link.evolution_current.verify().map_err(|error| {
                ProtocolError::Validation(format!(
                    "Virtual claim Evolution current is invalid: {error}"
                ))
            })?;
            validate_content_id("Virtual claim Evolution receipt", &link.receipt_id)?;
            for (kind, value) in [
                (
                    "Virtual claim Evolution occurrence",
                    &link.pin.occurrence_id,
                ),
                ("Virtual claim Evolution template", &link.pin.template_id),
                ("Virtual claim Evolution decision", &link.pin.decision_id),
                ("Virtual claim Evolution selection", &link.pin.selection_id),
            ] {
                validate_identity(kind, value)?;
            }
            validate_content_id("Virtual claim Evolution Plan", &link.pin.plan_id)?;
            validate_execution_binding(&link.pin.execution_binding)?;
            let expected_selection_id =
                crate::evolution::derive_virtual_evolution_selection_id(virtual_persistence_id)
                    .map_err(|error| {
                        ProtocolError::Validation(format!(
                            "Virtual claim Evolution selection identity is invalid: {error}"
                        ))
                    })?;
            if link.evolution_current.evolution_id != *evolution_id
                || link.evolution_current.last_receipt_id != link.receipt_id
                || link.pin.template_id != *template_id
                || link.pin.occurrence_id != claim.occurrence_id
                || link.pin.plan_id != claim.plan_id
                || link.pin.execution_binding != claim.execution_binding
                || link.pin.selection_id != expected_selection_id
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Virtual claim Evolution link changed its current, receipt, occurrence, Plan, or ExecutionBinding"
                        .to_owned(),
                ));
            }
            Ok(())
        }
        _ => Err(ProtocolError::IllegalTransition(
            "Virtual claim selection does not match its Run execution configuration and actual claim"
                .to_owned(),
        )),
    }
}

fn reduce_virtual_compaction(
    persistence: &VirtualCompactionPersistenceCommand,
    source: &VirtualKeyedSource,
    manifest: VirtualArchiveManifest,
    archive: VirtualCompactionPublication,
    archive_pin: ResourcePinReceipt,
) -> ProtocolResult<ReducedVirtualOperation> {
    let command = &persistence.command;
    let (current, before_region) =
        validate_virtual_compaction_selection(persistence, source, &manifest, &archive)?;
    let (occurrence_root, command_root) =
        validate_virtual_compaction_publication(&manifest, &archive)?;
    let certificate = build_virtual_compaction_certificate(
        command,
        current,
        &manifest,
        &archive,
        occurrence_root,
        command_root,
    )?;
    let (receipt, command_root_result) = build_virtual_compaction_receipt(
        command,
        current,
        &manifest,
        &archive,
        &archive_pin,
        &certificate,
    )?;
    let mut after_region = before_region.clone();
    after_region.hot_work_count = 0;
    after_region.hot_occurrence_count = 0;
    after_region.compaction_certificate_id = Some(certificate.certificate_id.clone());
    let mut mutations = vec![
        VirtualStateMutation::Regions {
            before: Some(before_region),
            after: Some(after_region),
        },
        VirtualStateMutation::Certificates {
            before: None,
            after: Some(Box::new(VirtualCertificateCurrent {
                leaf_version: VIRTUAL_CERTIFICATE_CURRENT_VERSION.to_owned(),
                scheduler_id: persistence.scheduler_id.clone(),
                certificate,
                lifecycle: VirtualCertificateLifecycle::Active,
            })),
        },
    ];
    mutations.extend(
        source
            .work
            .values()
            .cloned()
            .map(|before| VirtualStateMutation::Work {
                before: Some(before),
                after: None,
            }),
    );
    mutations.extend(source.occurrences.values().cloned().map(|before| {
        VirtualStateMutation::Occurrences {
            before: Some(Box::new(before)),
            after: None,
        }
    }));
    let mut counts = current.body.counts;
    counts.hot_work = checked_exact_sub(
        "Virtual hot work count",
        counts.hot_work,
        command.work_ids.len() as u64,
    )?;
    counts.hot_occurrences = checked_exact_sub(
        "Virtual hot occurrence count",
        counts.hot_occurrences,
        command.occurrence_ids.len() as u64,
    )?;
    counts.certificates = checked_exact_add("Virtual certificate count", counts.certificates, 1)?;
    let mut next = draft_from_current(current, current.body.frontier.clone(), counts);
    next.archived_work_index_root_digest = manifest.result_work_index_root_digest;
    next.archived_command_index_root_digest = command_root_result;
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::Compacted { archive },
        current: next,
        mutations,
        outcome: VirtualPersistenceOutcome::Compacted(receipt),
        artifacts: Vec::new(),
        archive_pin: Some(archive_pin),
        archive_release: None,
    })
}

fn validate_virtual_compaction_selection<'a>(
    persistence: &VirtualCompactionPersistenceCommand,
    source: &'a VirtualKeyedSource,
    manifest: &VirtualArchiveManifest,
    archive: &VirtualCompactionPublication,
) -> ProtocolResult<(&'a VirtualCurrent, VirtualRegionCurrent)> {
    let current = require_virtual_current(source)?;
    require_only_source_families(
        source,
        &[
            VirtualStateFamily::Regions,
            VirtualStateFamily::Work,
            VirtualStateFamily::Occurrences,
            VirtualStateFamily::Certificates,
        ],
    )?;
    manifest.verify()?;
    verify_compaction_publication(persistence, archive)?;
    let command = &persistence.command;
    if source.regions.keys().cloned().collect::<BTreeSet<_>>()
        != BTreeSet::from([command.region_id.clone()])
        || !source.certificates.is_empty()
        || source.work.keys().cloned().collect::<BTreeSet<_>>() != command.work_ids
        || source.occurrences.keys().cloned().collect::<BTreeSet<_>>() != command.occurrence_ids
        || manifest.work_index.keys().cloned().collect::<BTreeSet<_>>() != command.work_ids
        || manifest
            .occurrences
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != command.occurrence_ids
        || manifest
            .command_receipts
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != command.archived_command_ids
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual compaction did not load exactly its selected region, work, occurrences, receipts, and certificate absence"
                .to_owned(),
        ));
    }
    let before_region = source.regions[&command.region_id].clone();
    let region_is_closed = matches!(
        before_region.lifecycle,
        VirtualRegionLifecycle::Retired { .. }
    ) || before_region.region.cursor.exhausted;
    let expected_journal = (!manifest.command_receipts.is_empty())
        .then(|| virtual_scheduler_journal_id(&persistence.scheduler_id))
        .transpose()?;
    if !region_is_closed
        || command.archive != current.body.archive
        || before_region.compaction_certificate_id.is_some()
        || before_region.hot_work_count != command.work_ids.len() as u64
        || before_region.hot_occurrence_count != command.occurrence_ids.len() as u64
        || manifest.region_id != command.region_id
        || manifest.run_id != before_region.region.run_id
        || manifest.source_causal_cut != command.source_causal_cut
        || manifest.parent_work_index_root_digest != current.body.archived_work_index_root_digest
        || manifest.work_index_updates != archive.work_index_updates
        || manifest.journal_id != expected_journal
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual compaction product changed region closure, counts, cut, journal, or cumulative work parent"
                .to_owned(),
        ));
    }
    if current
        .body
        .frontier
        .ready
        .values()
        .flatten()
        .any(|item| command.work_ids.contains(&item.work_id))
        || current
            .body
            .frontier
            .active
            .keys()
            .any(|work_id| command.work_ids.contains(work_id))
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual compaction selection still contains ready or active work".to_owned(),
        ));
    }
    verify_virtual_compaction_hot_leaves(source, command, manifest)?;
    Ok((current, before_region))
}

fn verify_virtual_compaction_hot_leaves(
    source: &VirtualKeyedSource,
    command: &VirtualCompactionCommand,
    manifest: &VirtualArchiveManifest,
) -> ProtocolResult<()> {
    for (work_id, work) in &source.work {
        let archived = &manifest.work_index[work_id];
        if work.item.region_id != command.region_id
            || work.item.run_id != manifest.run_id
            || work.placement != VirtualWorkPlacement::Terminal
            || work.latest_occurrence_id.as_ref() != Some(&archived.occurrence_id)
            || work.max_epoch != archived.max_epoch
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual compaction work leaf changed its terminal archive fence".to_owned(),
            ));
        }
    }
    for (occurrence_id, occurrence) in &source.occurrences {
        if occurrence.occurrence != manifest.occurrences[occurrence_id] {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual compaction manifest changed an exact occurrence leaf".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_virtual_compaction_publication(
    manifest: &VirtualArchiveManifest,
    archive: &VirtualCompactionPublication,
) -> ProtocolResult<(String, Option<String>)> {
    let manifest_bytes = cymule_core::canonical_bytes(manifest)?;
    let expected_digest = format!("sha256:{}", cymule_core::sha256_bytes(&manifest_bytes));
    if !matches!(
        &archive.publication.resource.integrity,
        ResourceIntegrity::Content { digest, size }
            if digest == &expected_digest && *size == manifest_bytes.len() as u64
    ) {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive Resource does not address the exact typed manifest bytes".to_owned(),
        ));
    }
    let occurrence_root = virtual_archive_occurrence_root(&manifest.occurrences)?;
    let command_root =
        virtual_archive_command_root(manifest.journal_id.as_deref(), &manifest.command_receipts)?;
    if archive.occurrence_root_digest != occurrence_root
        || archive.command_root_digest != command_root
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive publication roots do not match the exact typed manifest".to_owned(),
        ));
    }
    Ok((occurrence_root, command_root))
}

fn build_virtual_compaction_certificate(
    command: &VirtualCompactionCommand,
    current: &VirtualCurrent,
    manifest: &VirtualArchiveManifest,
    archive: &VirtualCompactionPublication,
    occurrence_root: String,
    command_root: Option<String>,
) -> ProtocolResult<VirtualCompactionCertificate> {
    let retained_execution_bindings = manifest
        .occurrences
        .values()
        .map(|occurrence| occurrence.execution_binding.clone())
        .collect();
    let mut certificate = VirtualCompactionCertificate {
        certificate_version: VIRTUAL_COMPACTION_CERTIFICATE_VERSION.to_owned(),
        certificate_id: String::new(),
        source_causal_cut: command.source_causal_cut.clone(),
        summary: virtual_completion_summary(manifest)?,
        summary_state_digest: cymule_core::canonical_digest(manifest)?,
        occurrence_root_digest: occurrence_root,
        parent_work_index_root_digest: current.body.archived_work_index_root_digest.clone(),
        work_index_updates_digest: cymule_core::canonical_digest(&archive.work_index_updates)?,
        work_index_root_digest: manifest.result_work_index_root_digest.clone(),
        command_root_digest: command_root,
        command_count: manifest.command_receipts.len() as u64,
        unresolved_obligations: BTreeSet::new(),
        retained_execution_bindings,
        replay_availability: ReplayAvailability::Exact,
        rehydration_manifest: archive.publication.resource.clone(),
        archive: command.archive.clone(),
    };
    certificate.certificate_id = virtual_compaction_certificate_id(&certificate)?;
    certificate.verify()?;
    Ok(certificate)
}

fn build_virtual_compaction_receipt(
    command: &VirtualCompactionCommand,
    current: &VirtualCurrent,
    manifest: &VirtualArchiveManifest,
    archive: &VirtualCompactionPublication,
    archive_pin: &ResourcePinReceipt,
    certificate: &VirtualCompactionCertificate,
) -> ProtocolResult<(VirtualCompactionReceipt, String)> {
    let command_root_result = verify_compaction_command_updates(
        current,
        manifest,
        certificate,
        &archive.command_index_updates,
    )?;
    archive_pin
        .verify()
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let expected_subject = ResourceRetentionSubject::from_publication(&archive.publication)
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    if archive_pin.command_id != command.command_id
        || archive_pin.pin.subject != expected_subject
        || !matches!(
            &archive_pin.pin.kind,
            ResourcePinKind::VirtualArchive { archive_id }
                if archive_id == &certificate.rehydration_manifest.resource_id
        )
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual compaction archive pin changed command, Resource, or archive authority"
                .to_owned(),
        ));
    }
    let receipt = VirtualCompactionReceipt {
        command: command.clone(),
        certificate: certificate.clone(),
        resource_pin: archive_pin.clone(),
        parent_command_index_root_digest: current.body.archived_command_index_root_digest.clone(),
        command_index_updates_digest: cymule_core::canonical_digest(
            &archive.command_index_updates,
        )?,
        command_index_root_digest: command_root_result.clone(),
    };
    receipt.verify()?;
    Ok((receipt, command_root_result))
}

fn verify_compaction_command_updates(
    current: &VirtualCurrent,
    manifest: &VirtualArchiveManifest,
    certificate: &VirtualCompactionCertificate,
    updates: &[VirtualArchiveCommandIndexUpdate],
) -> ProtocolResult<String> {
    if updates.len() != manifest.command_receipts.len() {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive command locator count changed its typed receipt set".to_owned(),
        ));
    }
    let mut root = current.body.archived_command_index_root_digest.clone();
    for ((command_id, _), update) in manifest.command_receipts.iter().zip(updates) {
        let value = ArchivedCommandIndex {
            journal_id: manifest.journal_id.clone().ok_or_else(|| {
                ProtocolError::IllegalTransition(
                    "Virtual archived command requires its derived journal".to_owned(),
                )
            })?,
            command_id: command_id.clone(),
            certificate_id: certificate.certificate_id.clone(),
            archive_resource_id: certificate.rehydration_manifest.resource_id.clone(),
        };
        let (expected, nodes) =
            build_virtual_command_index_update(&root, update.nonmembership.clone(), &value)?;
        if nodes.is_empty() || update != &expected || update.parent_root_digest != root {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual archive command locator changed its exact insertion chain".to_owned(),
            ));
        }
        root.clone_from(&update.result_root_digest);
    }
    Ok(root)
}

fn virtual_completion_summary(
    manifest: &VirtualArchiveManifest,
) -> ProtocolResult<VirtualCompletionSummary> {
    let mut outputs = BTreeSet::new();
    let mut evidence = BTreeSet::new();
    let mut succeeded = 0_u64;
    let mut failed = 0_u64;
    let mut cancelled = 0_u64;
    for occurrence in manifest.occurrences.values() {
        match occurrence.state {
            WorkOccurrenceState::Succeeded => {
                succeeded = checked_exact_add("Virtual succeeded count", succeeded, 1)?;
                outputs.extend(occurrence.result.iter().cloned());
            }
            WorkOccurrenceState::Failed => {
                failed = checked_exact_add("Virtual failed count", failed, 1)?;
                evidence.extend(occurrence.error.iter().cloned());
            }
            WorkOccurrenceState::Cancelled => {
                cancelled = checked_exact_add("Virtual cancelled count", cancelled, 1)?;
                evidence.extend(occurrence.error.iter().cloned());
            }
            WorkOccurrenceState::Running
            | WorkOccurrenceState::RetryScheduled
            | WorkOccurrenceState::Parked => {}
        }
    }
    Ok(VirtualCompletionSummary {
        region_id: manifest.region_id.clone(),
        run_id: manifest.run_id.clone(),
        occurrence_count: manifest.occurrences.len() as u64,
        work_count: manifest.work_index.len() as u64,
        succeeded_count: succeeded,
        failed_count: failed,
        cancelled_count: cancelled,
        output_digest: cymule_core::canonical_digest(&outputs)?,
        evidence_digest: cymule_core::canonical_digest(&evidence)?,
        retained_debug_index_digest: cymule_core::canonical_digest(&manifest.work_index)?,
    })
}

fn reduce_virtual_rehydration(
    persistence: &VirtualRehydrationPersistenceCommand,
    source: &VirtualKeyedSource,
    occurrences: Vec<VirtualRehydratedOccurrence>,
) -> ProtocolResult<ReducedVirtualOperation> {
    let command = &persistence.command;
    let (current, certificate, before_region) =
        validate_virtual_rehydration(persistence, source, &occurrences)?;
    let region_id = &certificate.certificate.summary.region_id;

    let mut mutations = Vec::new();
    let mut inserted = 0_u64;
    for entry in &occurrences {
        let occurrence = &entry.occurrence;
        verify_occurrence_proof(
            &certificate.certificate.occurrence_root_digest,
            certificate.certificate.summary.occurrence_count,
            &entry.proof,
            occurrence,
        )?;
        if occurrence.region_id != *region_id
            || occurrence.run_id != certificate.certificate.summary.run_id
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual rehydrated occurrence escaped its certificate region or Run".to_owned(),
            ));
        }
        let after = VirtualOccurrenceCurrent {
            leaf_version: VIRTUAL_OCCURRENCE_CURRENT_VERSION.to_owned(),
            scheduler_id: persistence.scheduler_id.clone(),
            occurrence: occurrence.clone(),
        };
        match source.occurrences.get(&occurrence.occurrence_id) {
            Some(before) if before == &after => {}
            Some(_) => {
                return Err(ProtocolError::IdentityMismatch(
                    "Virtual rehydration collided with different hot occurrence authority"
                        .to_owned(),
                ));
            }
            None => {
                inserted = checked_exact_add("Virtual rehydrated occurrence count", inserted, 1)?;
                mutations.push(VirtualStateMutation::Occurrences {
                    before: None,
                    after: Some(Box::new(after)),
                });
            }
        }
    }

    let mut after_region = before_region.clone();
    after_region.hot_occurrence_count = checked_exact_add(
        "Virtual region hot occurrence count",
        after_region.hot_occurrence_count,
        inserted,
    )?;
    if inserted > 0 {
        mutations.push(VirtualStateMutation::Regions {
            before: Some(before_region),
            after: Some(after_region),
        });
    }
    let mut counts = current.body.counts;
    counts.hot_occurrences = checked_exact_add(
        "Virtual hot occurrence count",
        counts.hot_occurrences,
        inserted,
    )?;
    let receipt = VirtualRehydrationReceipt {
        command: command.clone(),
        restored_occurrence_ids: command.occurrence_ids.clone(),
    };
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::Rehydrated { occurrences },
        current: draft_from_current(current, current.body.frontier.clone(), counts),
        mutations,
        outcome: VirtualPersistenceOutcome::Rehydrated(receipt),
        artifacts: Vec::new(),
        archive_pin: None,
        archive_release: None,
    })
}

fn validate_virtual_rehydration<'a>(
    persistence: &VirtualRehydrationPersistenceCommand,
    source: &'a VirtualKeyedSource,
    occurrences: &[VirtualRehydratedOccurrence],
) -> ProtocolResult<(
    &'a VirtualCurrent,
    &'a VirtualCertificateCurrent,
    VirtualRegionCurrent,
)> {
    let current = require_virtual_current(source)?;
    require_only_source_families(
        source,
        &[
            VirtualStateFamily::Regions,
            VirtualStateFamily::Occurrences,
            VirtualStateFamily::Certificates,
        ],
    )?;
    let command = &persistence.command;
    verify_rehydration_evidence(persistence, occurrences)?;
    if source.certificates.keys().cloned().collect::<BTreeSet<_>>()
        != BTreeSet::from([command.certificate_id.clone()])
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual rehydration must load exactly its selected certificate".to_owned(),
        ));
    }
    let certificate = &source.certificates[&command.certificate_id];
    if !matches!(certificate.lifecycle, VirtualCertificateLifecycle::Active)
        || certificate.scheduler_id != persistence.scheduler_id
        || certificate.certificate.certificate_id != command.certificate_id
        || certificate.certificate.archive != current.body.archive
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual rehydration requires the exact active certificate".to_owned(),
        ));
    }
    let region_id = &certificate.certificate.summary.region_id;
    if source.regions.keys().cloned().collect::<BTreeSet<_>>()
        != BTreeSet::from([region_id.clone()])
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual rehydration must load exactly its certificate region".to_owned(),
        ));
    }
    let before_region = source.regions[region_id].clone();
    if before_region.scheduler_id != persistence.scheduler_id
        || before_region.region.run_id != certificate.certificate.summary.run_id
        || before_region.compaction_certificate_id.as_deref()
            != Some(command.certificate_id.as_str())
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual rehydration region changed its scheduler, Run, or certificate authority"
                .to_owned(),
        ));
    }
    if !source
        .occurrences
        .keys()
        .all(|occurrence_id| command.occurrence_ids.contains(occurrence_id))
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual rehydration loaded an orphan hot occurrence".to_owned(),
        ));
    }
    Ok((current, certificate, before_region))
}

fn reduce_virtual_archive_retirement(
    persistence: &VirtualArchiveRetirementPersistenceCommand,
    source: &VirtualKeyedSource,
    release: &ResourceArchiveRelease,
    receipt: ResourceReleaseReceipt,
) -> ProtocolResult<ReducedVirtualOperation> {
    let validated = validate_virtual_archive_retirement(persistence, source, release, &receipt)?;
    let mut after_certificate = validated.before_certificate.clone();
    after_certificate.lifecycle = VirtualCertificateLifecycle::Retired {
        receipt: Box::new(validated.retirement.clone()),
    };
    let mut after_region = validated.before_region.clone();
    after_region.compaction_certificate_id = None;
    Ok(ReducedVirtualOperation {
        evidence: VirtualPersistenceEvidence::None,
        current: draft_from_current(
            validated.current,
            validated.current.body.frontier.clone(),
            validated.current.body.counts,
        ),
        mutations: vec![
            VirtualStateMutation::Regions {
                before: Some(validated.before_region),
                after: Some(after_region),
            },
            VirtualStateMutation::Certificates {
                before: Some(Box::new(validated.before_certificate)),
                after: Some(Box::new(after_certificate)),
            },
        ],
        outcome: VirtualPersistenceOutcome::ArchiveRetired(validated.retirement),
        artifacts: Vec::new(),
        archive_pin: None,
        archive_release: Some(receipt),
    })
}

struct ValidatedArchiveRetirement<'a> {
    current: &'a VirtualCurrent,
    before_certificate: VirtualCertificateCurrent,
    before_region: VirtualRegionCurrent,
    retirement: VirtualArchiveRetirementReceipt,
}

fn validate_virtual_archive_retirement<'a>(
    persistence: &VirtualArchiveRetirementPersistenceCommand,
    source: &'a VirtualKeyedSource,
    release: &ResourceArchiveRelease,
    receipt: &ResourceReleaseReceipt,
) -> ProtocolResult<ValidatedArchiveRetirement<'a>> {
    let current = require_virtual_current(source)?;
    require_only_source_families(
        source,
        &[
            VirtualStateFamily::Regions,
            VirtualStateFamily::Certificates,
        ],
    )?;
    let command = &persistence.command;
    if source.certificates.keys().cloned().collect::<BTreeSet<_>>()
        != BTreeSet::from([command.certificate_id.clone()])
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual archive retirement must load exactly its certificate".to_owned(),
        ));
    }
    let before_certificate = source.certificates[&command.certificate_id].clone();
    if !matches!(
        before_certificate.lifecycle,
        VirtualCertificateLifecycle::Active
    ) || before_certificate.scheduler_id != persistence.scheduler_id
        || before_certificate.certificate.archive != current.body.archive
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual archive retirement requires the exact active certificate".to_owned(),
        ));
    }
    let certificate = &before_certificate.certificate;
    let region_id = &certificate.summary.region_id;
    if source.regions.keys().cloned().collect::<BTreeSet<_>>()
        != BTreeSet::from([region_id.clone()])
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual archive retirement must load exactly its certificate region".to_owned(),
        ));
    }
    let before_region = source.regions[region_id].clone();
    if !matches!(
        before_region.lifecycle,
        VirtualRegionLifecycle::Retired { .. }
    ) || before_region.compaction_certificate_id.as_deref()
        != Some(command.certificate_id.as_str())
        || before_region.hot_work_count != 0
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual archive retirement requires a retired empty region bound to the certificate"
                .to_owned(),
        ));
    }
    release
        .verify()
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    receipt
        .verify()
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let expected_release = command.release(&receipt.pin)?;
    let expected_digest = certificate
        .rehydration_manifest
        .integrity
        .content_digest()
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(
                "Virtual archive certificate lost content-addressed integrity".to_owned(),
            )
        })?;
    if release != &expected_release
        || receipt.command_id != command.command_id
        || receipt.release_id != release.release_id
        || receipt.pin.pin_id != release.pin_id
        || receipt.pin.subject.resource_id != certificate.rehydration_manifest.resource_id
        || receipt.pin.subject.family.content_digest != expected_digest
        || !matches!(
            &receipt.pin.kind,
            ResourcePinKind::VirtualArchive { archive_id }
                if archive_id == &certificate.rehydration_manifest.resource_id
        )
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive retirement changed its certificate-owned Resource release".to_owned(),
        ));
    }
    let retirement = VirtualArchiveRetirementReceipt {
        command: command.clone(),
        resource_release: receipt.clone(),
    };
    retirement.verify()?;
    Ok(ValidatedArchiveRetirement {
        current,
        before_certificate,
        before_region,
        retirement,
    })
}

fn require_virtual_current(source: &VirtualKeyedSource) -> ProtocolResult<&VirtualCurrent> {
    source.current.as_ref().ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Virtual non-initialization command requires an exact parent current".to_owned(),
        )
    })
}

fn source_is_empty(source: &VirtualKeyedSource) -> bool {
    source.regions.is_empty()
        && source.active_regions.is_empty()
        && source.parked.is_empty()
        && source.parked_index.is_empty()
        && source.work.is_empty()
        && source.occurrences.is_empty()
        && source.runs.is_empty()
        && source.migrations.is_empty()
        && source.certificates.is_empty()
}

fn require_only_source_families(
    source: &VirtualKeyedSource,
    allowed: &[VirtualStateFamily],
) -> ProtocolResult<()> {
    let allowed = allowed.iter().copied().collect::<BTreeSet<_>>();
    let unexpected = [
        (VirtualStateFamily::Regions, !source.regions.is_empty()),
        (
            VirtualStateFamily::ActiveRegions,
            !source.active_regions.is_empty(),
        ),
        (VirtualStateFamily::Parked, !source.parked.is_empty()),
        (
            VirtualStateFamily::ParkedIndex,
            !source.parked_index.is_empty(),
        ),
        (VirtualStateFamily::Work, !source.work.is_empty()),
        (
            VirtualStateFamily::Occurrences,
            !source.occurrences.is_empty(),
        ),
        (VirtualStateFamily::Runs, !source.runs.is_empty()),
        (
            VirtualStateFamily::Migrations,
            !source.migrations.is_empty(),
        ),
        (
            VirtualStateFamily::Certificates,
            !source.certificates.is_empty(),
        ),
    ]
    .into_iter()
    .find_map(|(family, present)| (present && !allowed.contains(&family)).then_some(family));
    if let Some(family) = unexpected {
        return Err(ProtocolError::IllegalTransition(format!(
            "Virtual reducer received an orphan {family:?} source leaf"
        )));
    }
    Ok(())
}

fn draft_from_current(
    current: &VirtualCurrent,
    frontier: VirtualFrontierCurrent,
    counts: VirtualCurrentCounts,
) -> VirtualCurrentDraft {
    VirtualCurrentDraft {
        scheduler_id: current.body.scheduler_id.clone(),
        limits: current.body.limits,
        scheduling_policy: current.body.scheduling_policy,
        archive: current.body.archive.clone(),
        frontier,
        archived_work_index_root_digest: current.body.archived_work_index_root_digest.clone(),
        archived_command_index_root_digest: current.body.archived_command_index_root_digest.clone(),
        counts,
    }
}

fn virtual_materialized_count(current: &VirtualCurrent) -> ProtocolResult<usize> {
    current
        .body
        .frontier
        .ready
        .values()
        .map(VecDeque::len)
        .sum::<usize>()
        .checked_add(current.body.frontier.active.len())
        .and_then(|value| {
            usize::try_from(current.body.counts.parked)
                .ok()
                .and_then(|parked| value.checked_add(parked))
        })
        .ok_or_else(|| {
            ProtocolError::Validation("Virtual materialized count overflowed".to_owned())
        })
}

fn checked_exact_add(kind: &str, left: u64, right: u64) -> ProtocolResult<u64> {
    left.checked_add(right)
        .filter(|value| *value <= cymule_core::MAX_EXACT_INTEGER)
        .ok_or_else(|| ProtocolError::Validation(format!("{kind} exceeds the exact integer range")))
}

fn checked_exact_sub(kind: &str, left: u64, right: u64) -> ProtocolResult<u64> {
    left.checked_sub(right)
        .ok_or_else(|| ProtocolError::IllegalTransition(format!("{kind} would become negative")))
}

fn insert_ready_frontier(
    frontier: &mut VirtualFrontierCurrent,
    item: WorkItem,
) -> ProtocolResult<()> {
    if frontier.active.contains_key(&item.work_id)
        || frontier
            .ready
            .values()
            .any(|queue| queue.iter().any(|current| current.work_id == item.work_id))
        || frontier.ready_since.contains_key(&item.work_id)
    {
        return Err(ProtocolError::IllegalTransition(format!(
            "Virtual ready insertion repeats work {}",
            item.work_id
        )));
    }
    frontier
        .ready_since
        .insert(item.work_id.clone(), frontier.dispatch_sequence);
    let queue = frontier.ready.entry(item.run_id.clone()).or_default();
    let index = queue
        .iter()
        .position(|current| current.priority < item.priority)
        .unwrap_or(queue.len());
    queue.insert(index, item);
    Ok(())
}

fn remove_ready_frontier(
    frontier: &mut VirtualFrontierCurrent,
    run_id: &str,
    work_id: &str,
) -> ProtocolResult<WorkItem> {
    let queue = frontier.ready.get_mut(run_id).ok_or_else(|| {
        ProtocolError::IllegalTransition(format!("Virtual ready Run {run_id} is missing"))
    })?;
    let index = queue
        .iter()
        .position(|item| item.work_id == work_id)
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(format!("Virtual ready work {work_id} is missing"))
        })?;
    let item = queue
        .remove(index)
        .ok_or_else(|| ProtocolError::Integrity {
            code: "virtual_ready_index_mismatch".to_owned(),
            message: "Virtual ready queue changed after exact index selection".to_owned(),
        })?;
    if queue.is_empty() {
        frontier.ready.remove(run_id);
    }
    if frontier.ready_since.remove(work_id).is_none() {
        return Err(ProtocolError::IllegalTransition(format!(
            "Virtual ready work {work_id} has no aging authority"
        )));
    }
    Ok(item)
}

fn collect_keyed_leaves<T>(
    leaves: Vec<T>,
    verify: impl Fn(&T) -> ProtocolResult<()>,
    key: impl Fn(&T) -> ProtocolResult<String>,
) -> ProtocolResult<BTreeMap<String, T>> {
    let mut keyed = BTreeMap::new();
    for leaf in leaves {
        verify(&leaf)?;
        let key = key(&leaf)?;
        if keyed.insert(key.clone(), leaf).is_some() {
            return Err(ProtocolError::IllegalTransition(format!(
                "Virtual keyed source repeats leaf {key}"
            )));
        }
    }
    Ok(keyed)
}

fn parked_index_local_key(leaf: &VirtualParkedIndexPage) -> ProtocolResult<String> {
    parked_index_local_key_for(&leaf.reason, leaf.page)
}

fn parked_index_local_key_for(reason: &ParkReason, page: u64) -> ProtocolResult<String> {
    cymule_core::content_id(VIRTUAL_PARKED_INDEX_PAGE_VERSION, &(reason, page))
        .map_err(ProtocolError::from)
}

impl VirtualPersistenceCommand {
    /// Seal one complete semantic operation into its persistence identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn new(operation: VirtualPersistenceOperation) -> ProtocolResult<Self> {
        let persistence_id =
            cymule_core::content_id(VIRTUAL_PERSISTENCE_COMMAND_VERSION, &operation)?;
        let command = Self {
            persistence_version: VIRTUAL_PERSISTENCE_COMMAND_VERSION.to_owned(),
            persistence_id,
            operation,
        };
        command.verify()?;
        Ok(command)
    }

    /// Semantic scheduler namespace whose physical journal is derived.
    pub fn scheduler_id(&self) -> &str {
        match &self.operation {
            VirtualPersistenceOperation::Initialize(command) => &command.scheduler_id,
            VirtualPersistenceOperation::Materialize(command) => &command.scheduler_id,
            VirtualPersistenceOperation::ActivateWait(command) => &command.scheduler_id,
            VirtualPersistenceOperation::Resolve(command) => &command.scheduler_id,
            VirtualPersistenceOperation::MigrateRegion(command) => &command.scheduler_id,
            VirtualPersistenceOperation::Compact(command) => &command.scheduler_id,
            VirtualPersistenceOperation::Rehydrate(command) => &command.scheduler_id,
            VirtualPersistenceOperation::Claim(command) => &command.scheduler_id,
            VirtualPersistenceOperation::RenewLease(command) => &command.scheduler_id,
            VirtualPersistenceOperation::Recover(command) => &command.scheduler_id,
            VirtualPersistenceOperation::SetRunWeight(command) => &command.scheduler_id,
            VirtualPersistenceOperation::RetireArchive(command) => &command.scheduler_id,
        }
    }

    /// Stable semantic operation identity used as the derived checkpoint ID.
    pub fn command_id(&self) -> &str {
        match &self.operation {
            VirtualPersistenceOperation::Initialize(command) => &command.command_id,
            VirtualPersistenceOperation::Materialize(command) => &command.command_id,
            VirtualPersistenceOperation::ActivateWait(command) => &command.command_id,
            VirtualPersistenceOperation::Resolve(command) => &command.command.command_id,
            VirtualPersistenceOperation::MigrateRegion(command) => &command.command_id,
            VirtualPersistenceOperation::Compact(command) => &command.command.command_id,
            VirtualPersistenceOperation::Rehydrate(command) => &command.command.command_id,
            VirtualPersistenceOperation::Claim(command) => &command.command.command_id,
            VirtualPersistenceOperation::RenewLease(command) => &command.command.command_id,
            VirtualPersistenceOperation::Recover(command) => &command.command.command_id,
            VirtualPersistenceOperation::SetRunWeight(command) => &command.command.command_id,
            VirtualPersistenceOperation::RetireArchive(command) => &command.command.command_id,
        }
    }

    /// Derive the only physical M1 application journal for this scheduler.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn journal_id(&self) -> ProtocolResult<String> {
        virtual_scheduler_journal_id(self.scheduler_id())
    }

    /// Verify the complete self-contained command shape and immutable records.
    /// Transition legality against the current snapshot remains the pure
    /// reducer's responsibility.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.persistence_version != VIRTUAL_PERSISTENCE_COMMAND_VERSION
            || self.persistence_id
                != cymule_core::content_id(VIRTUAL_PERSISTENCE_COMMAND_VERSION, &self.operation)?
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual persistence command version or complete content identity changed"
                    .to_owned(),
            ));
        }
        validate_identity("Virtual scheduler", self.scheduler_id())?;
        validate_identity("Virtual command", self.command_id())?;
        match &self.operation {
            VirtualPersistenceOperation::Initialize(command) => {
                verify_initialization_command(command)
            }
            VirtualPersistenceOperation::Materialize(command) => {
                verify_materialization_command(command)
            }
            VirtualPersistenceOperation::ActivateWait(command) => command.verify(),
            VirtualPersistenceOperation::Resolve(command) => {
                command.command.verify()?;
                verify_resolution_artifact(&command.command.resolution, command.artifact.as_ref())
            }
            VirtualPersistenceOperation::MigrateRegion(command) => {
                validate_identity("Virtual migration command", &command.command_id)?;
                verify_migration_request(&command.request)
            }
            VirtualPersistenceOperation::Compact(command) => command.command.verify(),
            VirtualPersistenceOperation::Rehydrate(command) => command.command.verify(),
            VirtualPersistenceOperation::Claim(command) => command.command.verify(),
            VirtualPersistenceOperation::RenewLease(command) => command.command.verify(),
            VirtualPersistenceOperation::Recover(command) => {
                command.command.verify()?;
                verify_resolution_artifact(&command.command.resolution, Some(&command.artifact))
            }
            VirtualPersistenceOperation::SetRunWeight(command) => command.command.verify(),
            VirtualPersistenceOperation::RetireArchive(command) => command.command.verify(),
        }?;
        if cymule_core::canonical_bytes(self)?.len() > MAX_VIRTUAL_PERSISTENCE_COMMAND_BYTES {
            return Err(ProtocolError::Validation(
                "Virtual persistence command exceeds its hard canonical byte bound".to_owned(),
            ));
        }
        Ok(())
    }
}

impl VirtualStateMutation {
    /// Return the normalized `StateRoot` family changed by this operation.
    pub const fn family(&self) -> VirtualStateFamily {
        match self {
            Self::Regions { .. } => VirtualStateFamily::Regions,
            Self::ActiveRegions { .. } => VirtualStateFamily::ActiveRegions,
            Self::Parked { .. } => VirtualStateFamily::Parked,
            Self::ParkedIndex { .. } => VirtualStateFamily::ParkedIndex,
            Self::Work { .. } => VirtualStateFamily::Work,
            Self::Occurrences { .. } => VirtualStateFamily::Occurrences,
            Self::Runs { .. } => VirtualStateFamily::Runs,
            Self::Migrations { .. } => VirtualStateFamily::Migrations,
            Self::Certificates { .. } => VirtualStateFamily::Certificates,
        }
    }

    /// Derive the unique global `StateRoot` storage key for this scheduler leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn storage_key(&self) -> ProtocolResult<String> {
        let (scheduler_id, local_key) = self.verify_coordinates()?;
        virtual_state_storage_key(&scheduler_id, self.family(), &local_key)
    }

    /// Return the exact parent leaf, absent only for an insertion.
    pub fn before_leaf(&self) -> Option<VirtualStateLeaf> {
        match self {
            Self::Regions { before, .. } => before.clone().map(VirtualStateLeaf::Regions),
            Self::ActiveRegions { before, .. } => {
                before.clone().map(VirtualStateLeaf::ActiveRegions)
            }
            Self::Parked { before, .. } => before.clone().map(VirtualStateLeaf::Parked),
            Self::ParkedIndex { before, .. } => before.clone().map(VirtualStateLeaf::ParkedIndex),
            Self::Work { before, .. } => before.clone().map(VirtualStateLeaf::Work),
            Self::Occurrences { before, .. } => before.clone().map(VirtualStateLeaf::Occurrences),
            Self::Runs { before, .. } => before.clone().map(VirtualStateLeaf::Runs),
            Self::Migrations { before, .. } => before.clone().map(VirtualStateLeaf::Migrations),
            Self::Certificates { before, .. } => before.clone().map(VirtualStateLeaf::Certificates),
        }
    }

    /// Return the exact resulting leaf, absent only for a deletion.
    pub fn after_leaf(&self) -> Option<VirtualStateLeaf> {
        match self {
            Self::Regions { after, .. } => after.clone().map(VirtualStateLeaf::Regions),
            Self::ActiveRegions { after, .. } => after.clone().map(VirtualStateLeaf::ActiveRegions),
            Self::Parked { after, .. } => after.clone().map(VirtualStateLeaf::Parked),
            Self::ParkedIndex { after, .. } => after.clone().map(VirtualStateLeaf::ParkedIndex),
            Self::Work { after, .. } => after.clone().map(VirtualStateLeaf::Work),
            Self::Occurrences { after, .. } => after.clone().map(VirtualStateLeaf::Occurrences),
            Self::Runs { after, .. } => after.clone().map(VirtualStateLeaf::Runs),
            Self::Migrations { after, .. } => after.clone().map(VirtualStateLeaf::Migrations),
            Self::Certificates { after, .. } => after.clone().map(VirtualStateLeaf::Certificates),
        }
    }

    /// Return the scheduler namespace sealed by this leaf transition.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn scheduler_id(&self) -> ProtocolResult<String> {
        self.verify_coordinates()
            .map(|(scheduler_id, _)| scheduler_id)
    }

    /// Verify exact before/after identity, scheduler ownership, and leaf shape.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        self.verify_coordinates().map(|_| ())
    }

    fn verify_coordinates(&self) -> ProtocolResult<(String, String)> {
        match self {
            Self::Regions { before, after } => verify_leaf_transition(
                "Virtual region",
                before.as_ref(),
                after.as_ref(),
                VirtualRegionCurrent::verify,
                |leaf| Ok((leaf.scheduler_id.clone(), leaf.region.region_id.clone())),
            ),
            Self::ActiveRegions { before, after } => verify_leaf_transition(
                "Virtual active region",
                before.as_ref(),
                after.as_ref(),
                VirtualActiveRegionCurrent::verify,
                |leaf| Ok((leaf.scheduler_id.clone(), leaf.region_id.clone())),
            ),
            Self::Parked { before, after } => verify_leaf_transition(
                "Virtual parked work",
                before.as_ref(),
                after.as_ref(),
                VirtualParkedCurrent::verify,
                |leaf| Ok((leaf.scheduler_id.clone(), leaf.parked.item.work_id.clone())),
            ),
            Self::ParkedIndex { before, after } => verify_leaf_transition(
                "Virtual parked-index page",
                before.as_ref(),
                after.as_ref(),
                VirtualParkedIndexPage::verify,
                |leaf| {
                    Ok((
                        leaf.scheduler_id.clone(),
                        cymule_core::content_id(
                            VIRTUAL_PARKED_INDEX_PAGE_VERSION,
                            &(&leaf.reason, leaf.page),
                        )?,
                    ))
                },
            ),
            Self::Work { before, after } => verify_leaf_transition(
                "Virtual work",
                before.as_ref(),
                after.as_ref(),
                VirtualWorkCurrent::verify,
                |leaf| Ok((leaf.scheduler_id.clone(), leaf.item.work_id.clone())),
            ),
            Self::Occurrences { before, after } => verify_leaf_transition(
                "Virtual occurrence",
                before.as_ref(),
                after.as_ref(),
                |leaf| leaf.verify(),
                |leaf| {
                    Ok((
                        leaf.scheduler_id.clone(),
                        leaf.occurrence.occurrence_id.clone(),
                    ))
                },
            ),
            Self::Runs { before, after } => verify_leaf_transition(
                "Virtual Run",
                before.as_ref(),
                after.as_ref(),
                VirtualRunCurrent::verify,
                |leaf| Ok((leaf.scheduler_id.clone(), leaf.run_id.clone())),
            ),
            Self::Migrations { before, after } => verify_leaf_transition(
                "Virtual migration",
                before.as_ref(),
                after.as_ref(),
                VirtualMigrationCurrent::verify,
                |leaf| {
                    Ok((
                        leaf.scheduler_id.clone(),
                        leaf.receipt.plan.migration_id.clone(),
                    ))
                },
            ),
            Self::Certificates { before, after } => verify_leaf_transition(
                "Virtual certificate",
                before.as_ref(),
                after.as_ref(),
                |leaf| leaf.verify(),
                |leaf| {
                    Ok((
                        leaf.scheduler_id.clone(),
                        leaf.certificate.certificate_id.clone(),
                    ))
                },
            ),
        }
    }
}

fn virtual_state_storage_key(
    scheduler_id: &str,
    family: VirtualStateFamily,
    local_key: &str,
) -> ProtocolResult<String> {
    validate_identity("Virtual scheduler", scheduler_id)?;
    validate_identity("Virtual state local key", local_key)?;
    cymule_core::content_id(
        VIRTUAL_STATE_STORAGE_KEY_DOMAIN,
        &(scheduler_id, family, local_key),
    )
    .map_err(ProtocolError::from)
}

impl VirtualMutationSet {
    /// Seal a deterministic unique family-and-key ordered mutation set.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn new(operations: Vec<VirtualStateMutation>) -> ProtocolResult<Self> {
        let mut keyed = operations
            .into_iter()
            .map(|operation| {
                operation.verify()?;
                Ok(((operation.family(), operation.storage_key()?), operation))
            })
            .collect::<ProtocolResult<Vec<_>>>()?;
        keyed.sort_by(|left, right| left.0.cmp(&right.0));
        let operations = keyed
            .into_iter()
            .map(|(_, operation)| operation)
            .collect::<Vec<_>>();
        let mutation_id = cymule_core::content_id(VIRTUAL_MUTATION_SET_VERSION, &operations)?;
        let mutations = Self {
            mutation_version: VIRTUAL_MUTATION_SET_VERSION.to_owned(),
            mutation_id,
            operations,
        };
        mutations.verify()?;
        Ok(mutations)
    }

    /// Verify bounded canonical ordering, uniqueness, and complete identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.mutation_version != VIRTUAL_MUTATION_SET_VERSION
            || self.operations.len() > MAX_VIRTUAL_MUTATION_SET_ITEMS
            || cymule_core::canonical_bytes(self)?.len() > MAX_VIRTUAL_MUTATION_BYTES
            || self.mutation_id
                != cymule_core::content_id(VIRTUAL_MUTATION_SET_VERSION, &self.operations)?
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual mutation set version, bound, or identity changed".to_owned(),
            ));
        }
        let mut previous = None;
        for operation in &self.operations {
            operation.verify()?;
            let key = (operation.family(), operation.storage_key()?);
            if previous.as_ref().is_some_and(|value| value >= &key) {
                return Err(ProtocolError::IllegalTransition(
                    "Virtual mutations must be strictly family-and-key ordered".to_owned(),
                ));
            }
            previous = Some(key);
        }
        Ok(())
    }
}

fn verify_leaf_transition<T: PartialEq>(
    kind: &str,
    before: Option<&T>,
    after: Option<&T>,
    verify: impl Fn(&T) -> ProtocolResult<()>,
    coordinates: impl Fn(&T) -> ProtocolResult<(String, String)>,
) -> ProtocolResult<(String, String)> {
    if before.is_none() && after.is_none() {
        return Err(ProtocolError::IllegalTransition(format!(
            "{kind} mutation cannot be empty"
        )));
    }
    if before == after {
        return Err(ProtocolError::IllegalTransition(format!(
            "{kind} mutation cannot retain an identical value"
        )));
    }
    let before_coordinates = before
        .map(|leaf| {
            verify(leaf)?;
            coordinates(leaf)
        })
        .transpose()?;
    let after_coordinates = after
        .map(|leaf| {
            verify(leaf)?;
            coordinates(leaf)
        })
        .transpose()?;
    if let (Some(before), Some(after)) = (&before_coordinates, &after_coordinates)
        && before != after
    {
        return Err(ProtocolError::IdentityMismatch(format!(
            "{kind} mutation changed scheduler ownership or storage identity"
        )));
    }
    before_coordinates.or(after_coordinates).ok_or_else(|| {
        ProtocolError::IllegalTransition(format!("{kind} mutation has no exact coordinates"))
    })
}

impl VirtualPersistenceReceipt {
    /// Seal the exact all-ever replay receipt for one reduced command.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn new(
        command: VirtualPersistenceCommand,
        parent_current_id: Option<String>,
        evidence: VirtualPersistenceEvidence,
        mutations: VirtualMutationSet,
        result_body_id: impl Into<String>,
        outcome: VirtualPersistenceOutcome,
    ) -> ProtocolResult<Self> {
        command.verify()?;
        verify_persistence_evidence(&command, &evidence)?;
        mutations.verify()?;
        verify_virtual_outcome(&command, &evidence, &outcome)?;
        if matches!(
            command.operation,
            VirtualPersistenceOperation::Initialize(_)
        ) != parent_current_id.is_none()
        {
            return Err(ProtocolError::IllegalTransition(
                "Virtual initialization alone may omit a parent current".to_owned(),
            ));
        }
        if let Some(parent) = &parent_current_id {
            validate_content_id("Virtual parent current", parent)?;
        }
        let result_body_id = result_body_id.into();
        validate_content_id("Virtual result current body", &result_body_id)?;
        let receipt_id = cymule_core::content_id(
            VIRTUAL_PERSISTENCE_RECEIPT_VERSION,
            &(
                &command,
                &parent_current_id,
                &evidence,
                &mutations,
                &result_body_id,
                &outcome,
            ),
        )?;
        let receipt = Self {
            receipt_version: VIRTUAL_PERSISTENCE_RECEIPT_VERSION.to_owned(),
            receipt_id,
            command,
            parent_current_id,
            evidence,
            mutations,
            result_body_id,
            outcome,
        };
        receipt.verify()?;
        Ok(receipt)
    }

    /// Verify the complete command, result, parent, and receipt identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        self.command.verify()?;
        verify_persistence_evidence(&self.command, &self.evidence)?;
        self.mutations.verify()?;
        verify_virtual_outcome(&self.command, &self.evidence, &self.outcome)?;
        validate_content_id("Virtual result current body", &self.result_body_id)?;
        if self.receipt_version != VIRTUAL_PERSISTENCE_RECEIPT_VERSION
            || self.receipt_id
                != cymule_core::content_id(
                    VIRTUAL_PERSISTENCE_RECEIPT_VERSION,
                    &(
                        &self.command,
                        &self.parent_current_id,
                        &self.evidence,
                        &self.mutations,
                        &self.result_body_id,
                        &self.outcome,
                    ),
                )?
            || (matches!(
                self.command.operation,
                VirtualPersistenceOperation::Initialize(_)
            ) != self.parent_current_id.is_none())
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual persistence receipt version, parent, or identity changed".to_owned(),
            ));
        }
        if let Some(parent) = &self.parent_current_id {
            validate_content_id("Virtual parent current", parent)?;
        }
        if self
            .mutations
            .operations
            .iter()
            .any(|operation| match operation.scheduler_id() {
                Ok(scheduler_id) => scheduler_id != self.command.scheduler_id(),
                Err(_) => true,
            })
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual persistence receipt contains a cross-scheduler mutation".to_owned(),
            ));
        }
        if cymule_core::canonical_bytes(self)?.len() > MAX_VIRTUAL_PERSISTENCE_RECEIPT_BYTES {
            return Err(ProtocolError::Validation(
                "Virtual persistence receipt exceeds the Durable StateRoot leaf-safe byte bound"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

impl VirtualCurrentQuery {
    /// Verify the scheduler partition and optional exact physical revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        verify_optional_virtual_revision(self.expected_revision.as_deref())
    }
}

impl VirtualReceiptQuery {
    /// Verify the scheduler, command, and optional exact physical revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        validate_identity("Virtual command", &self.command_id)?;
        verify_optional_virtual_revision(self.expected_revision.as_deref())
    }
}

impl VirtualCurrentRead {
    /// Verify revision pinning and exact scalar-current ownership.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify_for(&self, query: &VirtualCurrentQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_observed_virtual_revision(
            &self.observed_revision,
            query.expected_revision.as_deref(),
        )?;
        if let Some(current) = &self.current {
            current.verify()?;
            if current.body.scheduler_id != query.scheduler_id {
                return Err(ProtocolError::IdentityMismatch(
                    "Virtual current read changed its exact scheduler key".to_owned(),
                ));
            }
        }
        verify_virtual_control_envelope("Virtual current read", self)
    }
}

impl VirtualReceiptRead {
    /// Verify revision pinning and exact scheduler-and-command receipt ownership.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify_for(&self, query: &VirtualReceiptQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_observed_virtual_revision(
            &self.observed_revision,
            query.expected_revision.as_deref(),
        )?;
        if let Some(receipt) = &self.receipt {
            receipt.verify()?;
            if receipt.command.scheduler_id() != query.scheduler_id
                || receipt.command.command_id() != query.command_id
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Virtual receipt read changed its exact scheduler or command key".to_owned(),
                ));
            }
        }
        verify_virtual_control_envelope("Virtual receipt read", self)
    }
}

impl VirtualCommit {
    /// Verify one physical commit or exact replay envelope for a semantic command.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify_for(&self, command: &VirtualPersistenceCommand) -> ProtocolResult<()> {
        command.verify()?;
        validate_content_id(
            "Virtual observed StateRoot revision",
            &self.observed_revision,
        )?;
        if let Some(committed) = &self.committed_revision {
            validate_content_id("Virtual committed StateRoot revision", committed)?;
            if committed != &self.observed_revision {
                return Err(ProtocolError::IdentityMismatch(
                    "new Virtual commit did not return its resulting observed revision".to_owned(),
                ));
            }
        }
        self.receipt.verify()?;
        if &self.receipt.command != command {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual commit receipt belongs to a different semantic command".to_owned(),
            ));
        }
        verify_virtual_control_envelope("Virtual commit envelope", self)
    }
}

impl VirtualClaimOutcome {
    /// Construct the closed no-work result from an exact verified claim
    /// persistence receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the receipt is not a claim command or retained an
    /// actual claim, Run execution selector, or Evolution selection.
    pub fn no_work(receipt: VirtualPersistenceReceipt) -> ProtocolResult<Self> {
        let outcome = Self::NoWork {
            receipt: Box::new(receipt),
        };
        outcome.verify()?;
        Ok(outcome)
    }

    /// Construct the closed claimed-work result from an exact verified claim
    /// persistence receipt and the Plan loaded from the same pinned root.
    ///
    /// # Errors
    ///
    /// Returns an error when the receipt has no actual claim, the Plan is
    /// malformed or differs from the retained Plan identity, or any claim and
    /// binding authority differs from the receipt.
    pub fn claimed(
        receipt: VirtualPersistenceReceipt,
        plan: cymule_core::SealedPlan,
    ) -> ProtocolResult<Self> {
        let claim = virtual_claim_receipt(&receipt)?
            .claim
            .clone()
            .ok_or_else(|| {
                ProtocolError::IllegalTransition(
                    "claimed Virtual outcome has no actual claim".to_owned(),
                )
            })?;
        let outcome = Self::Claimed {
            receipt: Box::new(receipt),
            claim: Box::new(claim),
            plan: Box::new(plan),
        };
        outcome.verify()?;
        Ok(outcome)
    }

    /// Borrow the complete normalized persistence receipt.
    pub fn receipt(&self) -> &VirtualPersistenceReceipt {
        match self {
            Self::NoWork { receipt } | Self::Claimed { receipt, .. } => receipt,
        }
    }

    /// Verify the closed claim/no-work shape and every retained semantic
    /// cross-binding.
    ///
    /// # Errors
    ///
    /// Returns an error when the receipt, claim, Plan, or execution-binding
    /// identity is malformed, missing, extraneous, or inconsistent.
    pub fn verify(&self) -> ProtocolResult<()> {
        let persisted = virtual_claim_receipt(self.receipt())?;
        match self {
            Self::NoWork { .. } => {
                if persisted.claim.is_some()
                    || persisted.run_execution.is_some()
                    || persisted.evolution_selection.is_some()
                {
                    return Err(ProtocolError::IllegalTransition(
                        "no-work Virtual outcome retained claim-only authority".to_owned(),
                    ));
                }
            }
            Self::Claimed { claim, plan, .. } => {
                verify_claimed_work(claim)?;
                plan.verify()?;
                if persisted.claim.as_ref() != Some(claim.as_ref()) || plan.plan_id != claim.plan_id
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "claimed Virtual outcome changed its receipt claim or exact Plan"
                            .to_owned(),
                    ));
                }
            }
        }
        verify_virtual_control_envelope("Virtual claim outcome", self)
    }
}

fn virtual_claim_receipt(
    receipt: &VirtualPersistenceReceipt,
) -> ProtocolResult<&VirtualClaimReceipt> {
    receipt.verify()?;
    match (&receipt.command.operation, &receipt.outcome) {
        (VirtualPersistenceOperation::Claim(_), VirtualPersistenceOutcome::Claimed(claim)) => {
            Ok(claim)
        }
        _ => Err(ProtocolError::IllegalTransition(
            "Virtual claim outcome requires one exact claim persistence receipt".to_owned(),
        )),
    }
}

fn verify_optional_virtual_revision(revision: Option<&str>) -> ProtocolResult<()> {
    if let Some(revision) = revision {
        validate_content_id("Virtual StateRoot revision", revision)?;
    }
    Ok(())
}

fn verify_observed_virtual_revision(observed: &str, expected: Option<&str>) -> ProtocolResult<()> {
    validate_content_id("Virtual observed StateRoot revision", observed)?;
    if expected.is_some_and(|expected| expected != observed) {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual read revision does not match its exact query constraint".to_owned(),
        ));
    }
    Ok(())
}

fn verify_virtual_control_envelope(name: &str, value: &impl Serialize) -> ProtocolResult<()> {
    if cymule_core::canonical_bytes(value)?.len() > MAX_VIRTUAL_CONTROL_ENVELOPE_BYTES {
        return Err(ProtocolError::Validation(format!(
            "{name} exceeds the hard Durable control-envelope byte bound"
        )));
    }
    Ok(())
}

impl VirtualCurrentBody {
    /// Seal one receipt-independent normalized semantic projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the reducer draft, normalized roots, cumulative
    /// index roots, derived counts, or canonical body identity are invalid.
    pub fn new(draft: VirtualCurrentDraft, roots: VirtualStateRoots) -> ProtocolResult<Self> {
        let mut body = Self {
            body_version: VIRTUAL_CURRENT_BODY_VERSION.to_owned(),
            body_id: String::new(),
            scheduler_id: draft.scheduler_id,
            limits: draft.limits,
            scheduling_policy: draft.scheduling_policy,
            archive: draft.archive,
            frontier: draft.frontier,
            roots,
            archived_work_index_root_digest: draft.archived_work_index_root_digest,
            archived_command_index_root_digest: draft.archived_command_index_root_digest,
            counts: draft.counts,
        };
        body.body_id = virtual_current_body_id(&body)?;
        body.verify()?;
        Ok(body)
    }

    /// Verify semantic roots, bounded frontier, and complete body identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        verify_frontier_limits(self.limits)?;
        validate_scheduling_policy(self.scheduling_policy)?;
        validate_archive_binding(&self.archive)?;
        verify_virtual_state_roots(&self.roots)?;
        validate_content_id(
            "Virtual archived-work root",
            &self.archived_work_index_root_digest,
        )?;
        validate_content_id(
            "Virtual archived-command locator root",
            &self.archived_command_index_root_digest,
        )?;
        verify_virtual_current_counts(self.counts)?;
        verify_virtual_frontier(&self.frontier, self.limits, self.counts)?;
        if self.body_version != VIRTUAL_CURRENT_BODY_VERSION
            || self.body_id != virtual_current_body_id(self)?
            || cymule_core::canonical_bytes(self)?.len() > MAX_VIRTUAL_CURRENT_BYTES
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual current body version, bound, or complete content identity changed"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

impl VirtualCurrent {
    /// Seal one normalized current around a body and its producing receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn new(
        body: VirtualCurrentBody,
        last_receipt_id: impl Into<String>,
    ) -> ProtocolResult<Self> {
        body.verify()?;
        let mut current = Self {
            current_version: VIRTUAL_CURRENT_VERSION.to_owned(),
            current_id: String::new(),
            body,
            last_receipt_id: last_receipt_id.into(),
        };
        current.current_id = virtual_current_id(&current)?;
        current.verify()?;
        Ok(current)
    }

    /// Verify exact body/receipt linkage and complete current identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        self.body.verify()?;
        validate_content_id("Virtual persistence receipt", &self.last_receipt_id)?;
        let current_bytes = u64::try_from(cymule_core::canonical_bytes(self)?.len())
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        verify_virtual_wait_activation_source_budget(current_bytes, &self.body.frontier)?;
        if self.current_version != VIRTUAL_CURRENT_VERSION
            || self.current_id != virtual_current_id(self)?
            || current_bytes > MAX_VIRTUAL_CURRENT_BYTES as u64
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual current version, bound, Wait activation source budget, or complete content identity changed"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

impl VirtualRegionCurrent {
    /// Verify one exact normalized region leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_leaf_version(
            "Virtual region",
            &self.leaf_version,
            VIRTUAL_REGION_CURRENT_VERSION,
        )?;
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        validate_region(&self.region)?;
        if let VirtualRegionLifecycle::Retired { migration_id } = &self.lifecycle {
            validate_identity("Virtual region migration", migration_id)?;
        }
        validate_exact("Virtual region hot work count", self.hot_work_count)?;
        validate_exact(
            "Virtual region hot occurrence count",
            self.hot_occurrence_count,
        )?;
        if let Some(certificate_id) = &self.compaction_certificate_id {
            validate_content_id("Virtual compaction certificate", certificate_id)?;
        }
        verify_keyed_leaf_size("Virtual region", self)
    }
}

impl VirtualActiveRegionCurrent {
    /// Verify one exact materializable-region ordering leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_leaf_version(
            "Virtual active region",
            &self.leaf_version,
            VIRTUAL_ACTIVE_REGION_CURRENT_VERSION,
        )?;
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        validate_identity("Virtual active region", &self.region_id)?;
        verify_keyed_leaf_size("Virtual active region", self)
    }
}

impl VirtualWorkCurrent {
    /// Verify one exact normalized hot-work leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_leaf_version(
            "Virtual work",
            &self.leaf_version,
            VIRTUAL_WORK_CURRENT_VERSION,
        )?;
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        validate_work_item(&self.item)?;
        validate_exact("Virtual work maximum epoch", self.max_epoch)?;
        match (self.max_epoch, &self.latest_occurrence_id) {
            (0, None) => {}
            (epoch, Some(occurrence_id)) if epoch > 0 => {
                let expected = cymule_core::content_id(
                    VIRTUAL_WORK_OCCURRENCE_VERSION,
                    &(&self.item.work_id, epoch),
                )?;
                if occurrence_id != &expected {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual work leaf changed its latest occurrence fence".to_owned(),
                    ));
                }
            }
            _ => {
                return Err(ProtocolError::IllegalTransition(
                    "Virtual work epoch and latest occurrence presence disagree".to_owned(),
                ));
            }
        }
        if matches!(
            self.placement,
            VirtualWorkPlacement::Active
                | VirtualWorkPlacement::Parked
                | VirtualWorkPlacement::Terminal
        ) && self.max_epoch == 0
        {
            return Err(ProtocolError::IllegalTransition(
                "claimed, parked, or terminal Virtual work requires an occurrence fence".to_owned(),
            ));
        }
        verify_keyed_leaf_size("Virtual work", self)
    }
}

impl VirtualParkedCurrent {
    /// Verify one exact normalized parked-work leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_leaf_version(
            "Virtual parked work",
            &self.leaf_version,
            VIRTUAL_PARKED_CURRENT_VERSION,
        )?;
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        validate_work_item(&self.parked.item)?;
        verify_park_reason(&self.parked.reason)?;
        verify_keyed_leaf_size("Virtual parked work", self)
    }
}

impl VirtualParkedIndexPage {
    /// Verify one exact bounded parked-reason index page.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_leaf_version(
            "Virtual parked-index page",
            &self.page_version,
            VIRTUAL_PARKED_INDEX_PAGE_VERSION,
        )?;
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        verify_park_reason(&self.reason)?;
        validate_exact("Virtual parked-index page", self.page)?;
        if self.work_ids.is_empty()
            || self.work_ids.len() > MAX_VIRTUAL_PARKED_INDEX_PAGE_ITEMS
            || self
                .work_ids
                .iter()
                .any(|work_id| validate_identity("Virtual parked work", work_id).is_err())
        {
            return Err(ProtocolError::Validation(
                "Virtual parked-index page must contain a bounded non-empty exact identity set"
                    .to_owned(),
            ));
        }
        if let Some(next_page) = self.next_page {
            validate_exact("Virtual parked-index next page", next_page)?;
            if self.page.checked_add(1) != Some(next_page) {
                return Err(ProtocolError::IllegalTransition(
                    "Virtual parked-index page chain is not consecutive".to_owned(),
                ));
            }
        }
        verify_keyed_leaf_size("Virtual parked-index page", self)
    }
}

impl VirtualOccurrenceCurrent {
    /// Verify one exact normalized occurrence leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_leaf_version(
            "Virtual occurrence",
            &self.leaf_version,
            VIRTUAL_OCCURRENCE_CURRENT_VERSION,
        )?;
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        self.occurrence.verify()?;
        verify_keyed_leaf_size("Virtual occurrence", self)
    }
}

impl VirtualRunCurrent {
    /// Verify one exact normalized Run-fairness leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_leaf_version(
            "Virtual Run",
            &self.leaf_version,
            VIRTUAL_RUN_CURRENT_VERSION,
        )?;
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        validate_identity("Virtual Run", &self.run_id)?;
        self.execution.verify()?;
        if self.weight == 0 {
            return Err(ProtocolError::Validation(
                "Virtual Run weight must be positive".to_owned(),
            ));
        }
        validate_exact("Virtual Run deficit", self.deficit)?;
        verify_keyed_leaf_size("Virtual Run", self)
    }
}

impl VirtualMigrationCurrent {
    /// Verify one exact normalized migration leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_leaf_version(
            "Virtual migration",
            &self.leaf_version,
            VIRTUAL_MIGRATION_CURRENT_VERSION,
        )?;
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        self.receipt.plan.verify()?;
        let sources = self
            .receipt
            .plan
            .expected_sources
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let targets = self
            .receipt
            .plan
            .targets
            .iter()
            .map(|target| target.region_id.clone())
            .collect::<BTreeSet<_>>();
        if self.receipt.retired_regions != sources || self.receipt.active_targets != targets {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual migration receipt changed its exact source or target set".to_owned(),
            ));
        }
        verify_keyed_leaf_size("Virtual migration", self)
    }
}

impl VirtualCertificateCurrent {
    /// Verify one exact normalized certificate leaf.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_leaf_version(
            "Virtual certificate",
            &self.leaf_version,
            VIRTUAL_CERTIFICATE_CURRENT_VERSION,
        )?;
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        self.certificate.verify()?;
        if let VirtualCertificateLifecycle::Retired { receipt } = &self.lifecycle {
            receipt.verify()?;
            if receipt.command.certificate_id != self.certificate.certificate_id {
                return Err(ProtocolError::IdentityMismatch(
                    "Virtual certificate retirement changed certificate authority".to_owned(),
                ));
            }
        }
        verify_keyed_leaf_size("Virtual certificate", self)
    }
}

impl VirtualPostcondition {
    /// Verify exact current/receipt linkage and Artifact closure.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        self.receipt.verify()?;
        self.current.verify()?;
        if self.current.body.scheduler_id != self.receipt.command.scheduler_id()
            || self.current.last_receipt_id != self.receipt.receipt_id
            || self.current.body.body_id != self.receipt.result_body_id
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual current does not bind its exact persistence receipt and result body"
                    .to_owned(),
            ));
        }
        verify_persistence_artifacts(
            &self.receipt.command,
            &self.receipt.evidence,
            &self.artifacts,
        )?;
        match (
            &self.receipt.command.operation,
            &self.receipt.outcome,
            &self.archive_pin,
            &self.archive_release,
        ) {
            (
                VirtualPersistenceOperation::Compact(command),
                VirtualPersistenceOutcome::Compacted(receipt),
                Some(pin),
                None,
            ) => {
                pin.verify()
                    .map_err(|error| ProtocolError::Validation(error.to_string()))?;
                if pin != &receipt.resource_pin
                    || pin.command_id != command.command.command_id
                    || pin.pin.subject.resource_id
                        != receipt.certificate.rehydration_manifest.resource_id
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Virtual compaction postcondition changed its exact archive pin".to_owned(),
                    ));
                }
                Ok(())
            }
            (
                VirtualPersistenceOperation::RetireArchive(_),
                VirtualPersistenceOutcome::ArchiveRetired(receipt),
                None,
                Some(release),
            ) if release == &receipt.resource_release => Ok(()),
            (
                VirtualPersistenceOperation::Compact(_)
                | VirtualPersistenceOperation::RetireArchive(_),
                _,
                _,
                _,
            ) => Err(ProtocolError::IllegalTransition(
                "Virtual archive transition has a partial Resource lifecycle postcondition"
                    .to_owned(),
            )),
            (_, _, None, None) => Ok(()),
            _ => Err(ProtocolError::IllegalTransition(
                "non-archive Virtual transition cannot mutate Resource lifecycle authority"
                    .to_owned(),
            )),
        }
    }
}

fn verify_virtual_outcome(
    command: &VirtualPersistenceCommand,
    evidence: &VirtualPersistenceEvidence,
    outcome: &VirtualPersistenceOutcome,
) -> ProtocolResult<()> {
    match (&command.operation, evidence, outcome) {
        (
            VirtualPersistenceOperation::Initialize(command),
            VirtualPersistenceEvidence::None,
            VirtualPersistenceOutcome::Initialized { region_count },
        ) if *region_count == command.regions.len() as u64 => Ok(()),
        (
            VirtualPersistenceOperation::Materialize(command),
            VirtualPersistenceEvidence::Materialized { page, .. },
            VirtualPersistenceOutcome::Materialized {
                region_id,
                materialized,
            },
        ) if region_id == &command.region_id && *materialized == page.items.len() as u64 => Ok(()),
        (
            VirtualPersistenceOperation::ActivateWait(command),
            VirtualPersistenceEvidence::Activated { .. },
            VirtualPersistenceOutcome::Activated {
                activation_id,
                woken,
            },
        ) if activation_id == &command.activation_id
            && *woken <= cymule_core::MAX_EXACT_INTEGER =>
        {
            Ok(())
        }
        (
            VirtualPersistenceOperation::Resolve(command),
            VirtualPersistenceEvidence::None,
            VirtualPersistenceOutcome::Resolved(receipt),
        ) if receipt.command == command.command => Ok(()),
        (
            VirtualPersistenceOperation::MigrateRegion(_),
            VirtualPersistenceEvidence::Migrated { command, .. },
            VirtualPersistenceOutcome::Migrated(receipt),
        ) if receipt.plan == command.plan => Ok(()),
        (
            VirtualPersistenceOperation::Compact(command),
            VirtualPersistenceEvidence::Compacted { archive },
            VirtualPersistenceOutcome::Compacted(receipt),
        ) if receipt.command == command.command => verify_compaction_outcome(archive, receipt),
        (
            VirtualPersistenceOperation::Rehydrate(command),
            VirtualPersistenceEvidence::Rehydrated { .. },
            VirtualPersistenceOutcome::Rehydrated(receipt),
        ) if receipt.command == command.command
            && receipt.restored_occurrence_ids == command.command.occurrence_ids =>
        {
            Ok(())
        }
        (
            VirtualPersistenceOperation::Claim(operation),
            VirtualPersistenceEvidence::None,
            VirtualPersistenceOutcome::Claimed(receipt),
        ) if receipt.command == operation.command => {
            verify_virtual_claim_receipt(&command.persistence_id, receipt)
        }
        (
            VirtualPersistenceOperation::RenewLease(command),
            VirtualPersistenceEvidence::None,
            VirtualPersistenceOutcome::LeaseRenewed(receipt),
        ) if receipt.command == command.command => Ok(()),
        (
            VirtualPersistenceOperation::Recover(command),
            VirtualPersistenceEvidence::None,
            VirtualPersistenceOutcome::Recovered(receipt),
        ) if receipt.command == command.command => Ok(()),
        (
            VirtualPersistenceOperation::SetRunWeight(command),
            VirtualPersistenceEvidence::None,
            VirtualPersistenceOutcome::RunWeightSet(receipt),
        ) if receipt.command == command.command => Ok(()),
        (
            VirtualPersistenceOperation::RetireArchive(command),
            VirtualPersistenceEvidence::None,
            VirtualPersistenceOutcome::ArchiveRetired(receipt),
        ) if receipt.command == command.command => receipt.verify(),
        _ => Err(ProtocolError::IdentityMismatch(
            "Virtual persistence outcome does not match its admitted command".to_owned(),
        )),
    }
}

fn verify_compaction_outcome(
    archive: &VirtualCompactionPublication,
    receipt: &VirtualCompactionReceipt,
) -> ProtocolResult<()> {
    receipt.verify()?;
    let expected_work_root = archive.work_index_updates.last().map_or(
        receipt.certificate.parent_work_index_root_digest.as_str(),
        |update| update.result_root_digest.as_str(),
    );
    let expected_command_parent = archive.command_index_updates.first().map_or(
        receipt.parent_command_index_root_digest.as_str(),
        |update| update.parent_root_digest.as_str(),
    );
    let expected_command_root = archive.command_index_updates.last().map_or(
        receipt.parent_command_index_root_digest.as_str(),
        |update| update.result_root_digest.as_str(),
    );
    if receipt.certificate.rehydration_manifest != archive.publication.resource
        || receipt.certificate.occurrence_root_digest != archive.occurrence_root_digest
        || receipt.certificate.command_root_digest != archive.command_root_digest
        || receipt.certificate.work_index_updates_digest
            != cymule_core::canonical_digest(&archive.work_index_updates)?
        || receipt.certificate.work_index_root_digest != expected_work_root
        || receipt.certificate.command_count != archive.command_index_updates.len() as u64
        || receipt.parent_command_index_root_digest != expected_command_parent
        || receipt.command_index_updates_digest
            != cymule_core::canonical_digest(&archive.command_index_updates)?
        || receipt.command_index_root_digest != expected_command_root
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual compaction outcome changed its exact provider publication or cumulative index updates"
                .to_owned(),
        ));
    }
    Ok(())
}

fn verify_persistence_artifacts(
    command: &VirtualPersistenceCommand,
    evidence: &VirtualPersistenceEvidence,
    artifacts: &[ArtifactRecord],
) -> ProtocolResult<()> {
    let expected = match (&command.operation, evidence) {
        (VirtualPersistenceOperation::Initialize(command), VirtualPersistenceEvidence::None) => {
            command
                .source_artifacts
                .iter()
                .map(|record| record.reference.clone())
                .collect()
        }
        (
            VirtualPersistenceOperation::Materialize(_),
            VirtualPersistenceEvidence::Materialized { page, .. },
        ) => page
            .artifacts
            .iter()
            .map(|record| record.reference.clone())
            .collect(),
        (
            VirtualPersistenceOperation::ActivateWait(_),
            VirtualPersistenceEvidence::Activated { result, .. },
        ) => BTreeSet::from([result.reference.clone()]),
        (VirtualPersistenceOperation::Resolve(command), VirtualPersistenceEvidence::None) => {
            command
                .artifact
                .iter()
                .map(|record| record.reference.clone())
                .collect()
        }
        (
            VirtualPersistenceOperation::MigrateRegion(_),
            VirtualPersistenceEvidence::Migrated {
                coverage_evidence,
                target_source_artifacts,
                ..
            },
        ) => std::iter::once(coverage_evidence.reference.clone())
            .chain(
                target_source_artifacts
                    .iter()
                    .map(|record| record.reference.clone()),
            )
            .collect(),
        (VirtualPersistenceOperation::Recover(command), VirtualPersistenceEvidence::None) => {
            BTreeSet::from([command.artifact.reference.clone()])
        }
        (VirtualPersistenceOperation::Compact(_), VirtualPersistenceEvidence::Compacted { .. })
        | (
            VirtualPersistenceOperation::Rehydrate(_),
            VirtualPersistenceEvidence::Rehydrated { .. },
        )
        | (
            VirtualPersistenceOperation::RenewLease(_)
            | VirtualPersistenceOperation::Claim(_)
            | VirtualPersistenceOperation::SetRunWeight(_)
            | VirtualPersistenceOperation::RetireArchive(_),
            VirtualPersistenceEvidence::None,
        ) => BTreeSet::new(),
        _ => {
            return Err(ProtocolError::IllegalTransition(
                "Virtual persistence Artifact closure received mismatched evidence".to_owned(),
            ));
        }
    };
    verify_exact_artifact_set(artifacts, &expected, "Virtual persistence postcondition")
}

fn verify_persistence_evidence(
    command: &VirtualPersistenceCommand,
    evidence: &VirtualPersistenceEvidence,
) -> ProtocolResult<()> {
    match (&command.operation, evidence) {
        (
            VirtualPersistenceOperation::Initialize(_)
            | VirtualPersistenceOperation::Resolve(_)
            | VirtualPersistenceOperation::Claim(_)
            | VirtualPersistenceOperation::RenewLease(_)
            | VirtualPersistenceOperation::Recover(_)
            | VirtualPersistenceOperation::SetRunWeight(_)
            | VirtualPersistenceOperation::RetireArchive(_),
            VirtualPersistenceEvidence::None,
        ) => Ok(()),
        (
            VirtualPersistenceOperation::Materialize(command),
            VirtualPersistenceEvidence::Materialized {
                page,
                archived_work_proofs,
            },
        ) => verify_materialization_evidence(command, page, archived_work_proofs),
        (
            VirtualPersistenceOperation::ActivateWait(command),
            VirtualPersistenceEvidence::Activated { receipt, result },
        ) => {
            receipt.verify()?;
            if receipt.activation.activation_id != command.activation_id {
                return Err(ProtocolError::IdentityMismatch(
                    "Virtual activation evidence changed the admitted activation identity"
                        .to_owned(),
                ));
            }
            verify_exact_artifact_record(
                result,
                &receipt.activation.result,
                "Virtual wait activation",
            )
        }
        (
            VirtualPersistenceOperation::MigrateRegion(command),
            VirtualPersistenceEvidence::Migrated {
                command: migration,
                coverage_evidence,
                target_source_artifacts,
            },
        ) => verify_migration_evidence(
            command,
            migration,
            coverage_evidence,
            target_source_artifacts,
        ),
        (
            VirtualPersistenceOperation::Compact(command),
            VirtualPersistenceEvidence::Compacted { archive },
        ) => verify_compaction_publication(command, archive),
        (
            VirtualPersistenceOperation::Rehydrate(command),
            VirtualPersistenceEvidence::Rehydrated { occurrences },
        ) => verify_rehydration_evidence(command, occurrences),
        _ => Err(ProtocolError::IllegalTransition(
            "Virtual persistence evidence does not belong to its semantic command".to_owned(),
        )),
    }
}

fn verify_frontier_limits(limits: FrontierLimits) -> ProtocolResult<()> {
    if limits.max_materialized == 0
        || limits.max_active == 0
        || limits.max_active_per_run == 0
        || limits.materialize_batch == 0
        || limits.max_active > MAX_VIRTUAL_CURRENT_FRONTIER_ITEMS
        || limits.max_active_per_run > MAX_VIRTUAL_CURRENT_FRONTIER_ITEMS
        || limits.materialize_batch > MAX_VIRTUAL_MUTATION_ITEMS
        || [
            limits.max_materialized,
            limits.max_active,
            limits.max_active_per_run,
            limits.materialize_batch,
        ]
        .into_iter()
        .any(|value| value as u64 > cymule_core::MAX_EXACT_INTEGER)
    {
        return Err(ProtocolError::Validation(
            "Virtual frontier limits must be positive exact integers".to_owned(),
        ));
    }
    Ok(())
}

fn virtual_current_id(current: &VirtualCurrent) -> ProtocolResult<String> {
    let mut identity = current.clone();
    identity.current_id.clear();
    cymule_core::content_id(VIRTUAL_CURRENT_VERSION, &identity).map_err(ProtocolError::from)
}

fn virtual_current_body_id(body: &VirtualCurrentBody) -> ProtocolResult<String> {
    let mut identity = body.clone();
    identity.body_id.clear();
    cymule_core::content_id(VIRTUAL_CURRENT_BODY_VERSION, &identity).map_err(ProtocolError::from)
}

fn require_leaf_version(kind: &str, actual: &str, expected: &str) -> ProtocolResult<()> {
    if actual != expected {
        return Err(ProtocolError::Validation(format!(
            "unsupported {kind} leaf version {actual}"
        )));
    }
    Ok(())
}

fn verify_keyed_leaf_size(kind: &str, leaf: &impl Serialize) -> ProtocolResult<()> {
    if cymule_core::canonical_bytes(leaf)?.len() > MAX_VIRTUAL_KEYED_LEAF_BYTES {
        return Err(ProtocolError::Validation(format!(
            "{kind} leaf exceeds the hard canonical byte bound"
        )));
    }
    Ok(())
}

fn verify_virtual_state_roots(roots: &VirtualStateRoots) -> ProtocolResult<()> {
    for (kind, root) in [
        ("regions", &roots.regions),
        ("active regions", &roots.active_regions),
        ("parked work", &roots.parked),
        ("parked index", &roots.parked_index),
        ("work", &roots.work),
        ("occurrences", &roots.occurrences),
        ("Runs", &roots.runs),
        ("migrations", &roots.migrations),
        ("certificates", &roots.certificates),
    ] {
        validate_content_id(&format!("Virtual {kind} root"), root)?;
    }
    Ok(())
}

fn verify_virtual_current_counts(counts: VirtualCurrentCounts) -> ProtocolResult<()> {
    for (kind, count) in [
        ("region", counts.regions),
        ("active region", counts.active_regions),
        ("parked", counts.parked),
        ("hot work", counts.hot_work),
        ("hot occurrence", counts.hot_occurrences),
        ("Run", counts.runs),
        ("migration", counts.migrations),
        ("certificate", counts.certificates),
    ] {
        validate_exact(&format!("Virtual {kind} count"), count)?;
    }
    if counts.runs > counts.regions || counts.active_regions > counts.regions {
        return Err(ProtocolError::IllegalTransition(
            "Virtual current has more Run or active-region leaves than region leaves".to_owned(),
        ));
    }
    Ok(())
}

fn virtual_wait_activation_totals(
    frontier: &VirtualFrontierCurrent,
) -> ProtocolResult<(u64, u64, u64, u64)> {
    let mut work_items = 0_u64;
    let mut source_items = 0_u64;
    let mut source_bytes = 0_u64;
    let mut mutation_bytes = 0_u64;
    for (wait_id, capacity) in &frontier.wait_activations {
        validate_content_id("Virtual Wait activation directory key", wait_id)?;
        capacity.verify()?;
        work_items = checked_exact_add(
            "Virtual Wait activation aggregate work count",
            work_items,
            capacity.work_items,
        )?;
        source_items = checked_exact_add(
            "Virtual Wait activation aggregate source-item count",
            source_items,
            capacity.source_items()?,
        )?;
        source_bytes = checked_exact_add(
            "Virtual Wait activation aggregate source bytes",
            source_bytes,
            capacity.source_bytes,
        )?;
        mutation_bytes = checked_exact_add(
            "Virtual Wait activation aggregate mutation bytes",
            mutation_bytes,
            capacity.mutation_bytes,
        )?;
    }
    Ok((work_items, source_items, source_bytes, mutation_bytes))
}

fn virtual_mutation_set_encoded_bytes(
    operation_count: u64,
    operation_bytes: u64,
) -> ProtocolResult<u64> {
    let empty = VirtualMutationSet {
        mutation_version: VIRTUAL_MUTATION_SET_VERSION.to_owned(),
        mutation_id: format!("sha256:{}", "0".repeat(64)),
        operations: Vec::new(),
    };
    let empty_bytes = u64::try_from(cymule_core::canonical_bytes(&empty)?.len())
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    if operation_count == 0 {
        return Ok(empty_bytes);
    }
    empty_bytes
        .checked_sub(2)
        .and_then(|bytes| bytes.checked_add(operation_bytes))
        .and_then(|bytes| bytes.checked_add(operation_count - 1))
        .filter(|bytes| *bytes <= cymule_core::MAX_EXACT_INTEGER)
        .ok_or_else(|| {
            ProtocolError::Validation(
                "Virtual Wait activation mutation-set byte charge overflowed".to_owned(),
            )
        })
}

fn verify_virtual_wait_activation_source_budget(
    current_bytes: u64,
    frontier: &VirtualFrontierCurrent,
) -> ProtocolResult<()> {
    let (_, _, source_bytes, _) = virtual_wait_activation_totals(frontier)?;
    if current_bytes
        .checked_add(source_bytes)
        .is_none_or(|bytes| bytes > MAX_VIRTUAL_REDUCTION_SOURCE_BYTES as u64)
    {
        return Err(ProtocolError::Validation(
            "Virtual Wait activation exceeds the exact aggregate source byte bound".to_owned(),
        ));
    }
    Ok(())
}

fn verify_virtual_frontier(
    frontier: &VirtualFrontierCurrent,
    limits: FrontierLimits,
    counts: VirtualCurrentCounts,
) -> ProtocolResult<()> {
    let ready_count = frontier.ready.values().map(VecDeque::len).sum::<usize>();
    let frontier_count = ready_count
        .checked_add(frontier.active.len())
        .ok_or_else(|| ProtocolError::Validation("Virtual frontier count overflowed".to_owned()))?;
    let materialized_count = u64::try_from(frontier_count)
        .ok()
        .and_then(|value| value.checked_add(counts.parked))
        .ok_or_else(|| {
            ProtocolError::Validation("Virtual materialized count overflowed".to_owned())
        })?;
    if frontier_count > MAX_VIRTUAL_CURRENT_FRONTIER_ITEMS
        || materialized_count > limits.max_materialized as u64
        || frontier.active.len() > limits.max_active
        || frontier.dispatch_sequence > cymule_core::MAX_EXACT_INTEGER
        || counts.hot_work < materialized_count
        || counts.hot_occurrences < frontier.active.len() as u64
    {
        return Err(ProtocolError::Validation(
            "Virtual current exceeds its hard or configured frontier bounds".to_owned(),
        ));
    }
    if let Some(run_id) = &frontier.last_run {
        validate_identity("Virtual last Run", run_id)?;
    }
    if let Some(region_id) = &frontier.last_region {
        validate_identity("Virtual last region", region_id)?;
    }
    let mut ready_ids = BTreeSet::new();
    for (run_id, queue) in &frontier.ready {
        validate_identity("Virtual ready Run", run_id)?;
        for item in queue {
            validate_work_item(item)?;
            if item.run_id != *run_id || !ready_ids.insert(item.work_id.clone()) {
                return Err(ProtocolError::IllegalTransition(
                    "Virtual ready frontier changed Run ownership or repeated work".to_owned(),
                ));
            }
        }
    }
    if frontier
        .ready_since
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>()
        != ready_ids
        || frontier
            .ready_since
            .values()
            .any(|sequence| *sequence > frontier.dispatch_sequence)
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual ready-age authority does not exactly match ready work".to_owned(),
        ));
    }
    let mut active_per_run = BTreeMap::<String, usize>::new();
    let mut active_slots = BTreeSet::new();
    for (work_id, claim) in &frontier.active {
        verify_claimed_work(claim)?;
        if claim.item.work_id != *work_id
            || ready_ids.contains(work_id)
            || !active_slots.insert(claim.lease.resource.clone())
        {
            return Err(ProtocolError::IllegalTransition(
                "Virtual active frontier changed its exact work, owner, or slot authority"
                    .to_owned(),
            ));
        }
        *active_per_run.entry(claim.item.run_id.clone()).or_default() += 1;
    }
    if active_per_run
        .values()
        .any(|count| *count > limits.max_active_per_run)
    {
        return Err(ProtocolError::Validation(
            "Virtual current exceeds the per-Run active bound".to_owned(),
        ));
    }
    let (wait_work, wait_source_items, _, wait_mutation_bytes) =
        virtual_wait_activation_totals(frontier)?;
    let wait_mutation_items = wait_source_items;
    if wait_work > counts.parked
        || wait_source_items > MAX_VIRTUAL_REDUCTION_SOURCE_ITEMS as u64
        || wait_mutation_items > MAX_VIRTUAL_MUTATION_SET_ITEMS as u64
        || virtual_mutation_set_encoded_bytes(wait_mutation_items, wait_mutation_bytes)?
            > MAX_VIRTUAL_MUTATION_BYTES as u64
    {
        return Err(ProtocolError::Validation(
            "Virtual Wait activation directory exceeds its exact aggregate source or mutation bound"
                .to_owned(),
        ));
    }
    Ok(())
}

/// Derive the fixed physical journal identity for one semantic scheduler.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn virtual_scheduler_journal_id(scheduler_id: &str) -> ProtocolResult<String> {
    validate_identity("Virtual scheduler", scheduler_id)?;
    cymule_core::content_id(VIRTUAL_SCHEDULER_JOURNAL_ID_DOMAIN, &scheduler_id)
        .map_err(ProtocolError::from)
}

/// Derive the unique scalar-current `StateRoot` key for one scheduler.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn virtual_current_key(scheduler_id: &str) -> ProtocolResult<String> {
    validate_identity("Virtual scheduler", scheduler_id)?;
    cymule_core::content_id(VIRTUAL_CURRENT_STORAGE_KEY_DOMAIN, &scheduler_id)
        .map_err(ProtocolError::from)
}

/// Derive the unique all-ever receipt `StateRoot` key for one semantic command.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn virtual_receipt_key(scheduler_id: &str, command_id: &str) -> ProtocolResult<String> {
    validate_identity("Virtual scheduler", scheduler_id)?;
    validate_identity("Virtual command", command_id)?;
    cymule_core::content_id(
        VIRTUAL_RECEIPT_STORAGE_KEY_DOMAIN,
        &(scheduler_id, command_id),
    )
    .map_err(ProtocolError::from)
}

/// Derive the exact region-current key from its semantic scheduler and region.
///
/// # Errors
///
/// Returns an error when either semantic identity is invalid.
pub fn virtual_region_key(scheduler_id: &str, region_id: &str) -> ProtocolResult<String> {
    virtual_state_storage_key(scheduler_id, VirtualStateFamily::Regions, region_id)
}

/// Derive the exact work-current key from its semantic scheduler and work.
///
/// # Errors
///
/// Returns an error when either semantic identity is invalid.
pub fn virtual_work_key(scheduler_id: &str, work_id: &str) -> ProtocolResult<String> {
    virtual_state_storage_key(scheduler_id, VirtualStateFamily::Work, work_id)
}

/// Derive the exact occurrence-current key from its semantic identities.
///
/// # Errors
///
/// Returns an error when the scheduler or occurrence identity is invalid.
pub fn virtual_occurrence_key(scheduler_id: &str, occurrence_id: &str) -> ProtocolResult<String> {
    virtual_state_storage_key(scheduler_id, VirtualStateFamily::Occurrences, occurrence_id)
}

/// Derive the exact Run-current key from its semantic scheduler and Run.
///
/// # Errors
///
/// Returns an error when either semantic identity is invalid.
pub fn virtual_run_key(scheduler_id: &str, run_id: &str) -> ProtocolResult<String> {
    virtual_state_storage_key(scheduler_id, VirtualStateFamily::Runs, run_id)
}

/// Derive the exact certificate-current key from its semantic identities.
///
/// # Errors
///
/// Returns an error when the scheduler or certificate identity is invalid.
pub fn virtual_certificate_key(scheduler_id: &str, certificate_id: &str) -> ProtocolResult<String> {
    virtual_state_storage_key(
        scheduler_id,
        VirtualStateFamily::Certificates,
        certificate_id,
    )
}

/// Derive the unique authenticated active-region ordering key.
///
/// Durable uses this key only as the cursor for an authenticated successor
/// proof. Retired and exhausted region leaves are absent from this family, so
/// a materialization choice never scans the cumulative `Regions` history.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn virtual_active_region_key(scheduler_id: &str, region_id: &str) -> ProtocolResult<String> {
    virtual_state_storage_key(scheduler_id, VirtualStateFamily::ActiveRegions, region_id)
}

/// Seal one Durable physical map descriptor into its semantic Virtual family
/// root identity.
///
/// Empty maps are represented by `node = None, entries = 0`; non-empty maps
/// require a content-addressed physical node and a positive exact count. This
/// prevents a raw optional node digest from becoming a second, ambiguous root
/// authority.
///
/// # Errors
///
/// Returns an error when the operation violates its closed Virtual contract or
/// its exact identity, bounds, or authority evidence does not verify.
pub fn virtual_state_root_id(
    family: VirtualStateFamily,
    node: Option<&str>,
    entries: u64,
) -> ProtocolResult<String> {
    validate_exact("Virtual state-root entry count", entries)?;
    match (node, entries) {
        (None, 0) => {}
        (Some(node), entries) if entries > 0 => {
            validate_content_id("Virtual StateRoot map node", node)?;
        }
        _ => {
            return Err(ProtocolError::IllegalTransition(
                "Virtual StateRoot map node presence does not match its exact entry count"
                    .to_owned(),
            ));
        }
    }
    cymule_core::content_id(VIRTUAL_STATE_ROOT_ID_DOMAIN, &(family, node, entries))
        .map_err(ProtocolError::from)
}

impl WorkResolutionCommand {
    /// Verify one normal fenced-resolution intent independently of state.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.control_version != VIRTUAL_WORK_CONTROL_VERSION {
            return Err(ProtocolError::Validation(
                "unsupported Virtual work resolution control version".to_owned(),
            ));
        }
        validate_identity("Virtual resolution command", &self.command_id)?;
        validate_identity("Virtual work", &self.work_id)?;
        validate_identity("Virtual resolution owner", &self.owner)?;
        validate_positive_exact("Virtual work epoch", self.epoch)?;
        validate_positive_exact("Virtual lease epoch", self.expected_lease_epoch)?;
        self.clock.verify()?;
        verify_resolution(&self.resolution)
    }
}

impl VirtualActivationCommand {
    /// Construct one content-addressed M1 activation-consumption intent.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn new(
        scheduler_id: impl Into<String>,
        activation_id: impl Into<String>,
    ) -> ProtocolResult<Self> {
        let scheduler_id = scheduler_id.into();
        let activation_id = activation_id.into();
        let command_id = cymule_core::content_id(
            VIRTUAL_ACTIVATION_CONTROL_VERSION,
            &(scheduler_id.as_str(), activation_id.as_str()),
        )?;
        let command = Self {
            control_version: VIRTUAL_ACTIVATION_CONTROL_VERSION.to_owned(),
            scheduler_id,
            command_id,
            activation_id,
        };
        command.verify()?;
        Ok(command)
    }

    /// Verify the activation identity and complete command identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Virtual scheduler", &self.scheduler_id)?;
        validate_identity("Virtual wait activation", &self.activation_id)?;
        if self.control_version != VIRTUAL_ACTIVATION_CONTROL_VERSION
            || self.command_id
                != cymule_core::content_id(
                    VIRTUAL_ACTIVATION_CONTROL_VERSION,
                    &(self.scheduler_id.as_str(), self.activation_id.as_str()),
                )?
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual activation command version or complete identity changed".to_owned(),
            ));
        }
        Ok(())
    }
}

impl VirtualClaimCommand {
    /// Verify one claim intent without accepting a caller-authored lease fence.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.control_version != VIRTUAL_CLAIM_CONTROL_VERSION {
            return Err(ProtocolError::Validation(
                "unsupported Virtual claim control version".to_owned(),
            ));
        }
        validate_identity("Virtual claim command", &self.command_id)?;
        validate_identity("Virtual claim owner", &self.owner)?;
        validate_identity("Virtual capacity slot", &self.slot_id)?;
        validate_execution_binding(&self.execution_binding)?;
        for capability in &self.capabilities {
            validate_identity("Virtual claim capability", capability)?;
        }
        self.clock.verify()?;
        if self.clock.scope != self.slot_id {
            return Err(ProtocolError::Validation(
                "Virtual claim Clock scope must equal its capacity slot".to_owned(),
            ));
        }
        validate_positive_exact("Virtual claim lease TTL", self.lease_ttl)
    }
}

impl VirtualLeaseRenewalCommand {
    /// Verify one active-claim renewal intent independently of current state.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.control_version != VIRTUAL_LEASE_RENEWAL_CONTROL_VERSION {
            return Err(ProtocolError::Validation(
                "unsupported Virtual lease-renewal control version".to_owned(),
            ));
        }
        validate_identity("Virtual lease-renewal command", &self.command_id)?;
        validate_identity("Virtual work", &self.work_id)?;
        validate_identity("Virtual lease owner", &self.owner)?;
        validate_positive_exact("Virtual work epoch", self.epoch)?;
        validate_positive_exact("Virtual expected lease epoch", self.expected_lease_epoch)?;
        validate_positive_exact("Virtual renewal lease TTL", self.lease_ttl)?;
        Ok(self.clock.verify()?)
    }
}

impl VirtualRecoveryCommand {
    /// Verify one explicit expired-claim recovery intent.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.control_version != VIRTUAL_RECOVERY_CONTROL_VERSION {
            return Err(ProtocolError::Validation(
                "unsupported Virtual recovery control version".to_owned(),
            ));
        }
        validate_identity("Virtual recovery command", &self.command_id)?;
        validate_identity("Virtual work", &self.work_id)?;
        validate_identity("Virtual recovery owner", &self.expected_owner)?;
        validate_positive_exact("Virtual recovery work epoch", self.expected_epoch)?;
        validate_positive_exact("Virtual recovery lease epoch", self.expected_lease_epoch)?;
        self.clock.verify()?;
        if !matches!(
            self.resolution,
            WorkResolution::Retry { .. }
                | WorkResolution::Failed { .. }
                | WorkResolution::Cancelled { .. }
        ) {
            return Err(ProtocolError::IllegalTransition(
                "Virtual recovery must explicitly retry, fail, or cancel".to_owned(),
            ));
        }
        verify_resolution(&self.resolution)
    }
}

impl VirtualRunWeightCommand {
    /// Verify one future-only Run scheduling-weight intent.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.control_version != VIRTUAL_RUN_WEIGHT_CONTROL_VERSION {
            return Err(ProtocolError::Validation(
                "unsupported Virtual Run-weight control version".to_owned(),
            ));
        }
        validate_identity("Virtual Run-weight command", &self.command_id)?;
        validate_identity("Virtual Run", &self.run_id)?;
        if self.weight == 0 {
            return Err(ProtocolError::Validation(
                "Virtual Run weight must be positive".to_owned(),
            ));
        }
        Ok(())
    }
}

impl RegionMigrationPlan {
    /// Verify the complete provider-produced migration shape.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.migration_version != VIRTUAL_REGION_MIGRATION_VERSION {
            return Err(ProtocolError::Validation(
                "unsupported Virtual region migration version".to_owned(),
            ));
        }
        validate_identity("Virtual migration", &self.migration_id)?;
        validate_identity("Virtual migration binding", &self.migration_binding)?;
        validate_identity("Virtual migration revision", &self.migration_revision)?;
        if self.expected_sources.is_empty() {
            return Err(ProtocolError::Validation(
                "Virtual migration requires at least one source".to_owned(),
            ));
        }
        if self.expected_sources.len() > MAX_VIRTUAL_MUTATION_ITEMS
            || self.targets.len() > MAX_VIRTUAL_MUTATION_ITEMS
        {
            return Err(ProtocolError::Validation(
                "Virtual migration exceeds the hard mutation-item bound".to_owned(),
            ));
        }
        for (region_id, source) in &self.expected_sources {
            validate_identity("Virtual migration source", region_id)?;
            validate_source_binding(&source.source)?;
            validate_cursor(&source.cursor)?;
        }
        validate_migration_cardinality(self.kind, self.expected_sources.len(), self.targets.len())?;
        let source_ids = self
            .expected_sources
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut target_ids = BTreeSet::new();
        for target in &self.targets {
            validate_region(target)?;
            if !target_ids.insert(target.region_id.clone())
                || source_ids.contains(&target.region_id)
            {
                return Err(ProtocolError::Validation(
                    "Virtual migration target IDs must be unique and disjoint from sources"
                        .to_owned(),
                ));
            }
        }
        self.coverage_evidence
            .validate()
            .map_err(ProtocolError::from)
    }
}

impl RegionMigrationCommand {
    /// Verify one idempotent region-migration command.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.control_version != VIRTUAL_REGION_MIGRATION_CONTROL_VERSION {
            return Err(ProtocolError::Validation(
                "unsupported Virtual region-migration control version".to_owned(),
            ));
        }
        validate_identity("Virtual migration command", &self.command_id)?;
        self.plan.verify()
    }
}

impl VirtualCompactionCommand {
    /// Construct one complete content-addressed compaction intent.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn new(
        region_id: impl Into<String>,
        source_causal_cut: BTreeSet<String>,
        work_ids: BTreeSet<String>,
        occurrence_ids: BTreeSet<String>,
        archived_command_ids: BTreeSet<String>,
        archive: VirtualArchiveBinding,
    ) -> ProtocolResult<Self> {
        let mut command = Self {
            control_version: VIRTUAL_COMPACTION_CONTROL_VERSION.to_owned(),
            command_id: String::new(),
            region_id: region_id.into(),
            source_causal_cut,
            work_ids,
            occurrence_ids,
            archived_command_ids,
            archive,
        };
        command.command_id = virtual_compaction_command_id(&command)?;
        command.verify()?;
        Ok(command)
    }

    /// Verify one immutable cold-history compaction intent.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.control_version != VIRTUAL_COMPACTION_CONTROL_VERSION {
            return Err(ProtocolError::Validation(
                "unsupported Virtual compaction control version".to_owned(),
            ));
        }
        validate_content_id("Virtual compaction command", &self.command_id)?;
        validate_identity("Virtual compaction region", &self.region_id)?;
        if self.source_causal_cut.is_empty() {
            return Err(ProtocolError::Validation(
                "Virtual compaction requires a non-empty causal cut".to_owned(),
            ));
        }
        for checkpoint_id in &self.source_causal_cut {
            validate_identity("Virtual compaction causal checkpoint", checkpoint_id)?;
        }
        if self.work_ids.is_empty()
            || self.occurrence_ids.is_empty()
            || self.work_ids.len() > MAX_VIRTUAL_MUTATION_ITEMS
            || self.occurrence_ids.len() > MAX_VIRTUAL_MUTATION_ITEMS
            || self.archived_command_ids.len() > MAX_VIRTUAL_MUTATION_ITEMS
        {
            return Err(ProtocolError::Validation(
                "Virtual compaction requires bounded non-empty work and occurrence selections"
                    .to_owned(),
            ));
        }
        for work_id in &self.work_ids {
            validate_identity("Virtual compacted work", work_id)?;
        }
        for occurrence_id in &self.occurrence_ids {
            validate_content_id("Virtual compacted occurrence", occurrence_id)?;
        }
        for command_id in &self.archived_command_ids {
            validate_identity("Virtual archived command", command_id)?;
        }
        validate_archive_binding(&self.archive)?;
        if self.command_id != virtual_compaction_command_id(self)? {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual compaction command identity does not match its complete intent".to_owned(),
            ));
        }
        Ok(())
    }
}

impl VirtualCompactionCertificate {
    /// Verify immutable certificate shape and complete content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        self.rehydration_manifest
            .verify()
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        if self.certificate_version != VIRTUAL_COMPACTION_CERTIFICATE_VERSION
            || self.certificate_id != virtual_compaction_certificate_id(self)?
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual compaction certificate version or identity changed".to_owned(),
            ));
        }
        if self.source_causal_cut.is_empty() {
            return Err(ProtocolError::Validation(
                "Virtual compaction certificate requires a causal cut".to_owned(),
            ));
        }
        for checkpoint_id in &self.source_causal_cut {
            validate_identity("Virtual compaction causal checkpoint", checkpoint_id)?;
        }
        validate_identity("Virtual compaction region", &self.summary.region_id)?;
        validate_identity("Virtual compaction Run", &self.summary.run_id)?;
        validate_archive_binding(&self.archive)?;
        for digest in [
            &self.summary_state_digest,
            &self.summary.output_digest,
            &self.summary.evidence_digest,
            &self.summary.retained_debug_index_digest,
            &self.work_index_updates_digest,
        ] {
            validate_canonical_digest("Virtual compaction", digest)?;
        }
        for digest in [
            &self.occurrence_root_digest,
            &self.parent_work_index_root_digest,
            &self.work_index_root_digest,
        ] {
            validate_content_id("Virtual compaction root", digest)?;
        }
        if let Some(root) = &self.command_root_digest {
            validate_content_id("Virtual archived-command root", root)?;
        }
        if (self.command_count == 0) != self.command_root_digest.is_none() {
            return Err(ProtocolError::Validation(
                "Virtual archived-command count and root presence disagree".to_owned(),
            ));
        }
        for value in [
            self.summary.occurrence_count,
            self.summary.work_count,
            self.summary.succeeded_count,
            self.summary.failed_count,
            self.summary.cancelled_count,
            self.command_count,
        ] {
            validate_exact("Virtual compaction count", value)?;
        }
        let terminal_count = self
            .summary
            .succeeded_count
            .checked_add(self.summary.failed_count)
            .and_then(|value| value.checked_add(self.summary.cancelled_count));
        if terminal_count != Some(self.summary.work_count)
            || !self.unresolved_obligations.is_empty()
            || self.replay_availability != ReplayAvailability::Exact
            || self.rehydration_manifest.media_type != VIRTUAL_ARCHIVE_MANIFEST_KIND
            || self.rehydration_manifest.shape != ResourceShape::Object
            || !matches!(
                self.rehydration_manifest.integrity,
                ResourceIntegrity::Content { .. }
            )
        {
            return Err(ProtocolError::Validation(
                "Virtual compaction certificate summary or Resource shape is malformed".to_owned(),
            ));
        }
        for binding in &self.retained_execution_bindings {
            validate_execution_binding(binding)?;
        }
        Ok(())
    }
}

impl VirtualCompactionReceipt {
    /// Verify the exact command-to-certificate relation.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        self.command.verify()?;
        self.certificate.verify()?;
        self.resource_pin
            .verify()
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        if self.resource_pin.command_id != self.command.command_id
            || self.resource_pin.pin.subject.resource_id
                != self.certificate.rehydration_manifest.resource_id
            || !matches!(
                &self.resource_pin.pin.kind,
                ResourcePinKind::VirtualArchive { archive_id }
                    if archive_id == &self.certificate.rehydration_manifest.resource_id
            )
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual compaction receipt changed its exact Resource archive pin".to_owned(),
            ));
        }
        for root in [
            &self.parent_command_index_root_digest,
            &self.command_index_root_digest,
        ] {
            validate_content_id("Virtual archived-command locator root", root)?;
        }
        validate_canonical_digest(
            "Virtual archived-command locator updates",
            &self.command_index_updates_digest,
        )?;
        let empty_updates =
            cymule_core::canonical_digest(&Vec::<VirtualArchiveCommandIndexUpdate>::new())?;
        if self.certificate.command_count == 0 {
            if self.parent_command_index_root_digest != self.command_index_root_digest
                || self.command_index_updates_digest != empty_updates
            {
                return Err(ProtocolError::IllegalTransition(
                    "command-free Virtual compaction changed the cumulative command locator"
                        .to_owned(),
                ));
            }
        } else if self.parent_command_index_root_digest == self.command_index_root_digest {
            return Err(ProtocolError::IllegalTransition(
                "Virtual compaction archived commands without advancing their locator root"
                    .to_owned(),
            ));
        }
        if self.command.region_id != self.certificate.summary.region_id
            || self.command.source_causal_cut != self.certificate.source_causal_cut
            || self.command.archive != self.certificate.archive
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual compaction receipt certificate changed its admitted command".to_owned(),
            ));
        }
        Ok(())
    }
}

impl VirtualRehydrationCommand {
    /// Verify one exact bounded cold occurrence selection.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.control_version != VIRTUAL_REHYDRATION_CONTROL_VERSION {
            return Err(ProtocolError::Validation(
                "unsupported Virtual rehydration control version".to_owned(),
            ));
        }
        validate_identity("Virtual rehydration command", &self.command_id)?;
        validate_content_id("Virtual compaction certificate", &self.certificate_id)?;
        if self.occurrence_ids.is_empty() || self.occurrence_ids.len() > MAX_VIRTUAL_MUTATION_ITEMS
        {
            return Err(ProtocolError::Validation(
                "Virtual rehydration requires a bounded non-empty occurrence set".to_owned(),
            ));
        }
        for occurrence_id in &self.occurrence_ids {
            validate_identity("Virtual rehydration occurrence", occurrence_id)?;
        }
        Ok(())
    }
}

impl WorkOccurrence {
    /// Verify one binding-pinned work occurrence independently of a snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation violates its closed Virtual contract or
    /// its exact identity, bounds, or authority evidence does not verify.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.occurrence_version != VIRTUAL_WORK_OCCURRENCE_VERSION
            || self.occurrence_id
                != cymule_core::content_id(
                    VIRTUAL_WORK_OCCURRENCE_VERSION,
                    &(&self.work_id, self.epoch),
                )?
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual work occurrence version or identity changed".to_owned(),
            ));
        }
        for (kind, value) in [
            ("Virtual occurrence", self.occurrence_id.as_str()),
            ("Virtual work", self.work_id.as_str()),
            ("Virtual region", self.region_id.as_str()),
            ("Virtual Run", self.run_id.as_str()),
            ("Virtual occurrence owner", self.owner.as_str()),
        ] {
            validate_identity(kind, value)?;
        }
        validate_content_id("Virtual occurrence Plan", &self.plan_id)?;
        validate_positive_exact("Virtual occurrence epoch", self.epoch)?;
        validate_positive_exact("Virtual occurrence lease epoch", self.lease_epoch)?;
        self.lease_clock.verify()?;
        validate_execution_binding(&self.execution_binding)?;
        let shape_matches = match self.state {
            WorkOccurrenceState::Running => {
                self.result.is_none() && self.error.is_none() && self.next_reason.is_none()
            }
            WorkOccurrenceState::Succeeded => {
                self.result
                    .as_ref()
                    .is_some_and(|value| value.validate().is_ok())
                    && self.error.is_none()
                    && self.next_reason.is_none()
            }
            WorkOccurrenceState::RetryScheduled => {
                self.result.is_none()
                    && self
                        .error
                        .as_ref()
                        .is_some_and(|value| value.validate().is_ok())
                    && self
                        .next_reason
                        .as_ref()
                        .is_none_or(|reason| verify_park_reason(reason).is_ok())
            }
            WorkOccurrenceState::Parked => {
                self.result.is_none()
                    && self.error.is_none()
                    && self
                        .next_reason
                        .as_ref()
                        .is_some_and(|reason| verify_park_reason(reason).is_ok())
            }
            WorkOccurrenceState::Failed | WorkOccurrenceState::Cancelled => {
                self.result.is_none()
                    && self
                        .error
                        .as_ref()
                        .is_some_and(|value| value.validate().is_ok())
                    && self.next_reason.is_none()
            }
        };
        if !shape_matches {
            return Err(ProtocolError::Validation(
                "Virtual occurrence fields do not match its lifecycle state".to_owned(),
            ));
        }
        Ok(())
    }
}

fn verify_initialization_command(command: &VirtualInitializationCommand) -> ProtocolResult<()> {
    if command.control_version != VIRTUAL_INITIALIZATION_CONTROL_VERSION {
        return Err(ProtocolError::Validation(
            "unsupported Virtual initialization control version".to_owned(),
        ));
    }
    validate_identity("Virtual initialization command", &command.command_id)?;
    validate_frontier_limits(command.limits)?;
    validate_scheduling_policy(command.scheduling_policy)?;
    validate_archive_binding(&command.archive)?;
    if command.regions.is_empty()
        || command.regions.len() > MAX_VIRTUAL_MUTATION_ITEMS
        || command.runs.is_empty()
        || command.runs.len() > MAX_VIRTUAL_MUTATION_ITEMS
    {
        return Err(ProtocolError::Validation(
            "Virtual initialization requires bounded non-empty region and Run sets".to_owned(),
        ));
    }
    let mut region_ids = BTreeSet::new();
    let mut region_run_ids = BTreeSet::new();
    let mut expected_sources = BTreeSet::new();
    for region in &command.regions {
        validate_region(region)?;
        if !region_ids.insert(region.region_id.as_str()) {
            return Err(ProtocolError::Validation(
                "Virtual initialization repeats a region identity".to_owned(),
            ));
        }
        region_run_ids.insert(region.run_id.as_str());
        expected_sources.insert(region.source_artifact.clone());
    }
    let mut run_ids = BTreeSet::new();
    let mut previous = None::<&str>;
    for run in &command.runs {
        run.verify()?;
        if previous.is_some_and(|previous| previous >= run.run_id.as_str())
            || !run_ids.insert(run.run_id.as_str())
        {
            return Err(ProtocolError::Validation(
                "Virtual initialization Runs must be strictly identity ordered and unique"
                    .to_owned(),
            ));
        }
        previous = Some(&run.run_id);
    }
    if run_ids != region_run_ids {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual initialization Run definitions do not exactly cover region Run references"
                .to_owned(),
        ));
    }
    verify_exact_artifact_set(
        &command.source_artifacts,
        &expected_sources,
        "Virtual initialization source",
    )
}

fn verify_migration_request(request: &RegionMigrationRequest) -> ProtocolResult<()> {
    validate_identity("Virtual migration", &request.migration_id)?;
    validate_identity("Virtual migration binding", &request.migration_binding)?;
    validate_identity("Virtual migration revision", &request.migration_revision)?;
    if request.source_region_ids.is_empty()
        || request.source_region_ids.len() > MAX_VIRTUAL_MUTATION_ITEMS
        || request.target_count == 0
        || request.target_count > MAX_VIRTUAL_MUTATION_ITEMS
        || request
            .source_region_ids
            .iter()
            .any(|region_id| validate_identity("Virtual migration source", region_id).is_err())
    {
        return Err(ProtocolError::Validation(
            "Virtual migration request has an invalid bounded source or target set".to_owned(),
        ));
    }
    validate_migration_cardinality(
        request.kind,
        request.source_region_ids.len(),
        request.target_count,
    )
}

fn verify_migration_evidence(
    persistence: &VirtualMigrationPersistenceCommand,
    command: &RegionMigrationCommand,
    coverage_evidence: &ArtifactRecord,
    target_source_artifacts: &[ArtifactRecord],
) -> ProtocolResult<()> {
    validate_identity("Virtual scheduler", &persistence.scheduler_id)?;
    verify_migration_request(&persistence.request)?;
    command.verify()?;
    let request = &persistence.request;
    let plan = &command.plan;
    if command.command_id != persistence.command_id
        || plan.migration_id != request.migration_id
        || plan.kind != request.kind
        || plan.migration_binding != request.migration_binding
        || plan.migration_revision != request.migration_revision
        || plan.expected_sources.keys().collect::<BTreeSet<_>>()
            != request.source_region_ids.iter().collect::<BTreeSet<_>>()
        || plan.targets.len() != request.target_count
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual migration evidence changed its semantic request".to_owned(),
        ));
    }
    if target_source_artifacts.len() > MAX_VIRTUAL_MUTATION_ITEMS {
        return Err(ProtocolError::Validation(
            "Virtual migration exceeds the bounded target-source Artifact count".to_owned(),
        ));
    }
    let mut material_references = BTreeSet::new();
    let mut material_bytes = 0_usize;
    for record in std::iter::once(coverage_evidence).chain(target_source_artifacts) {
        if !material_references.insert(&record.reference) {
            return Err(ProtocolError::Validation(
                "Virtual migration contains a duplicate coverage or target-source Artifact"
                    .to_owned(),
            ));
        }
        material_bytes = material_bytes
            .checked_add(record.bytes.len())
            .filter(|bytes| *bytes <= MAX_MATERIALIZED_PAGE_ARTIFACT_BYTES)
            .ok_or_else(|| {
                ProtocolError::Validation(
                    "Virtual migration exceeded the aggregate Artifact byte product".to_owned(),
                )
            })?;
    }
    verify_exact_artifact_record(
        coverage_evidence,
        &plan.coverage_evidence,
        "Virtual migration coverage",
    )?;
    let expected_sources = plan
        .targets
        .iter()
        .map(|target| target.source_artifact.clone())
        .collect::<BTreeSet<_>>();
    verify_exact_artifact_set(
        target_source_artifacts,
        &expected_sources,
        "Virtual migration target source",
    )
}

fn verify_materialization_command(command: &VirtualMaterializationCommand) -> ProtocolResult<()> {
    if command.control_version != VIRTUAL_MATERIALIZATION_CONTROL_VERSION {
        return Err(ProtocolError::Validation(
            "unsupported Virtual materialization control version".to_owned(),
        ));
    }
    validate_identity("Virtual materialization command", &command.command_id)?;
    validate_identity("Virtual materialization region", &command.region_id)?;
    validate_source_binding(&command.expected_source)?;
    validate_cursor(&command.expected_cursor)
}

fn verify_materialization_evidence(
    command: &VirtualMaterializationCommand,
    page: &MaterializedPage,
    archived_work_proofs: &BTreeMap<String, VirtualArchiveWorkProof>,
) -> ProtocolResult<()> {
    validate_cursor(&page.next_cursor)?;
    if page.items.len() > MAX_VIRTUAL_MUTATION_ITEMS
        || archived_work_proofs.len() > MAX_VIRTUAL_MUTATION_ITEMS
    {
        return Err(ProtocolError::Validation(
            "Virtual materialization exceeds the hard mutation-item bound".to_owned(),
        ));
    }
    if page.next_cursor.version != command.expected_cursor.version
        || (page.next_cursor == command.expected_cursor && !page.next_cursor.exhausted)
    {
        return Err(ProtocolError::IllegalTransition(
            "Virtual materialization changed cursor version or returned a nonterminal stall"
                .to_owned(),
        ));
    }
    let mut work_ids = BTreeSet::new();
    let mut run_id = None::<&str>;
    let mut payloads = BTreeSet::new();
    for item in &page.items {
        validate_work_item(item)?;
        if item.region_id != command.region_id || !work_ids.insert(item.work_id.as_str()) {
            return Err(ProtocolError::Validation(
                "Virtual materialization repeats work or escapes its selected region".to_owned(),
            ));
        }
        if run_id.is_some_and(|expected| expected != item.run_id) {
            return Err(ProtocolError::Validation(
                "Virtual materialization page spans multiple Runs".to_owned(),
            ));
        }
        run_id = Some(&item.run_id);
        payloads.insert(item.payload.clone());
    }
    if page.artifacts.len() > page.items.len() {
        return Err(ProtocolError::Validation(
            "Virtual materialization has more Artifact records than work items".to_owned(),
        ));
    }
    verify_artifact_subset(&page.artifacts, &payloads, "Virtual materialization")?;
    if archived_work_proofs.len() != work_ids.len() {
        return Err(ProtocolError::Validation(
            "Virtual materialization must carry one archived-work absence proof per item"
                .to_owned(),
        ));
    }
    for (work_id, proof) in archived_work_proofs {
        if !work_ids.contains(work_id.as_str())
            || proof.work_id != *work_id
            || proof.value.is_some()
        {
            return Err(ProtocolError::Validation(
                "Virtual materialization has an orphan or non-absence archived-work proof"
                    .to_owned(),
            ));
        }
        verify_work_proof_shape(proof)?;
    }
    Ok(())
}

fn verify_compaction_publication(
    command: &VirtualCompactionPersistenceCommand,
    archive: &VirtualCompactionPublication,
) -> ProtocolResult<()> {
    command.command.verify()?;
    if archive.work_index_updates.len() > MAX_VIRTUAL_MUTATION_ITEMS
        || archive.command_index_updates.len() > MAX_VIRTUAL_MUTATION_ITEMS
    {
        return Err(ProtocolError::Validation(
            "Virtual compaction exceeds the hard mutation-item bound".to_owned(),
        ));
    }
    archive
        .publication
        .verify()
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    if archive.publication.resource.media_type != VIRTUAL_ARCHIVE_MANIFEST_KIND
        || archive.publication.resource.shape != ResourceShape::Object
        || !matches!(
            archive.publication.resource.integrity,
            ResourceIntegrity::Content { .. }
        )
    {
        return Err(ProtocolError::Validation(
            "Virtual compaction publication is not an immutable archive object".to_owned(),
        ));
    }
    validate_content_id(
        "Virtual compaction occurrence root",
        &archive.occurrence_root_digest,
    )?;
    if let Some(root) = &archive.command_root_digest {
        validate_content_id("Virtual compaction command root", root)?;
    }
    let mut work_ids = BTreeSet::new();
    let mut previous_work_root = None::<&str>;
    for update in &archive.work_index_updates {
        validate_content_id(
            "Virtual archived-work parent root",
            &update.parent_root_digest,
        )?;
        validate_content_id(
            "Virtual archived-work result root",
            &update.result_root_digest,
        )?;
        verify_work_proof_shape(&update.nonmembership)?;
        verify_archived_work_index(&update.value)?;
        let (expected, _) = build_virtual_work_index_update(
            &update.parent_root_digest,
            update.nonmembership.clone(),
            &update.value,
        )?;
        if update.nonmembership.value.is_some()
            || update.nonmembership.work_id != update.value.work_id
            || expected != *update
            || previous_work_root.is_some_and(|parent| parent != update.parent_root_digest)
            || !work_ids.insert(update.value.work_id.as_str())
        {
            return Err(ProtocolError::Validation(
                "Virtual compaction work-index updates are not unique absence insertions"
                    .to_owned(),
            ));
        }
        previous_work_root = Some(&update.result_root_digest);
    }
    let mut command_keys = BTreeSet::new();
    let mut previous_command_root = None::<&str>;
    for update in &archive.command_index_updates {
        validate_content_id(
            "Virtual archived-command locator parent root",
            &update.parent_root_digest,
        )?;
        validate_content_id(
            "Virtual archived-command locator result root",
            &update.result_root_digest,
        )?;
        verify_command_index_proof_shape(&update.nonmembership)?;
        verify_archived_command_index(&update.value)?;
        let (expected, _) = build_virtual_command_index_update(
            &update.parent_root_digest,
            update.nonmembership.clone(),
            &update.value,
        )?;
        if update.nonmembership.value.is_some()
            || update.nonmembership.journal_id != update.value.journal_id
            || update.nonmembership.command_id != update.value.command_id
            || expected != *update
            || previous_command_root.is_some_and(|parent| parent != update.parent_root_digest)
            || !command_keys.insert((
                update.value.journal_id.as_str(),
                update.value.command_id.as_str(),
            ))
        {
            return Err(ProtocolError::Validation(
                "Virtual compaction command-index updates are not unique absence insertions"
                    .to_owned(),
            ));
        }
        previous_command_root = Some(&update.result_root_digest);
    }
    Ok(())
}

fn verify_rehydration_evidence(
    command: &VirtualRehydrationPersistenceCommand,
    occurrences: &[VirtualRehydratedOccurrence],
) -> ProtocolResult<()> {
    command.command.verify()?;
    if occurrences.len() > MAX_VIRTUAL_MUTATION_ITEMS {
        return Err(ProtocolError::Validation(
            "Virtual rehydration exceeds the hard mutation-item bound".to_owned(),
        ));
    }
    let mut selected = BTreeSet::new();
    for entry in occurrences {
        entry.occurrence.verify()?;
        verify_occurrence_proof_shape(&entry.proof)?;
        if entry.proof.occurrence_id != entry.occurrence.occurrence_id
            || !command
                .command
                .occurrence_ids
                .contains(&entry.occurrence.occurrence_id)
            || !selected.insert(entry.occurrence.occurrence_id.as_str())
        {
            return Err(ProtocolError::Validation(
                "Virtual rehydration contains an orphan, duplicate, or mismatched occurrence"
                    .to_owned(),
            ));
        }
        let bytes = cymule_core::canonical_bytes(&entry.occurrence)?;
        if entry.proof.length != bytes.len() as u64
            || entry.proof.digest != format!("sha256:{}", cymule_core::sha256_bytes(&bytes))
        {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual rehydration occurrence bytes changed their range proof".to_owned(),
            ));
        }
    }
    if selected.len() != command.command.occurrence_ids.len() {
        return Err(ProtocolError::Validation(
            "Virtual rehydration must carry exactly every selected occurrence".to_owned(),
        ));
    }
    Ok(())
}

fn verify_resolution_artifact(
    resolution: &WorkResolution,
    record: Option<&ArtifactRecord>,
) -> ProtocolResult<()> {
    let expected = match resolution {
        WorkResolution::Succeeded { result } => Some(result),
        WorkResolution::Retry { error, .. } | WorkResolution::Failed { error } => Some(error),
        WorkResolution::Parked { .. } => None,
        WorkResolution::Cancelled { reason } => Some(reason),
    };
    match (expected, record) {
        (Some(expected), Some(record)) => {
            verify_exact_artifact_record(record, expected, "Virtual resolution")
        }
        (None, None) => Ok(()),
        _ => Err(ProtocolError::Validation(
            "Virtual resolution Artifact presence does not match its disposition".to_owned(),
        )),
    }
}

fn verify_resolution(resolution: &WorkResolution) -> ProtocolResult<()> {
    match resolution {
        WorkResolution::Succeeded { result } => result.validate().map_err(ProtocolError::from),
        WorkResolution::Retry { error, next_reason } => {
            error.validate().map_err(ProtocolError::from)?;
            if let Some(reason) = next_reason {
                verify_park_reason(reason)?;
            }
            Ok(())
        }
        WorkResolution::Parked { reason } => verify_park_reason(reason),
        WorkResolution::Failed { error } => error.validate().map_err(ProtocolError::from),
        WorkResolution::Cancelled { reason } => reason.validate().map_err(ProtocolError::from),
    }
}

fn verify_park_reason(reason: &ParkReason) -> ProtocolResult<()> {
    match reason {
        ParkReason::Wait { key } => validate_content_id("Virtual parked Wait", key),
        ParkReason::Dependency { work_id } => validate_identity("Virtual park reason", work_id),
        ParkReason::Budget { account } => validate_identity("Virtual park reason", account),
        ParkReason::Capability { capability } => {
            validate_identity("Virtual park reason", capability)
        }
        ParkReason::Backpressure { domain } => validate_identity("Virtual park reason", domain),
    }
}

fn verify_exact_artifact_record(
    record: &ArtifactRecord,
    expected: &ArtifactRef,
    operation: &str,
) -> ProtocolResult<()> {
    expected.validate().map_err(ProtocolError::from)?;
    record.reference.validate().map_err(ProtocolError::from)?;
    let derived = cymule_core::artifact_ref(record.reference.kind.clone(), &record.bytes)?;
    if derived != record.reference || &record.reference != expected {
        return Err(ProtocolError::IdentityMismatch(format!(
            "{operation} Artifact bytes do not match the exact admitted reference"
        )));
    }
    Ok(())
}

fn verify_exact_artifact_set(
    records: &[ArtifactRecord],
    expected: &BTreeSet<ArtifactRef>,
    operation: &str,
) -> ProtocolResult<()> {
    let mut observed = BTreeSet::new();
    let mut bytes = 0_usize;
    for record in records {
        if !expected.contains(&record.reference) || !observed.insert(record.reference.clone()) {
            return Err(ProtocolError::Validation(format!(
                "{operation} contains a duplicate or orphan Artifact record"
            )));
        }
        verify_exact_artifact_record(record, &record.reference, operation)?;
        bytes = bytes.checked_add(record.bytes.len()).ok_or_else(|| {
            ProtocolError::Validation(format!("{operation} Artifact bytes overflowed"))
        })?;
    }
    if &observed != expected {
        return Err(ProtocolError::Validation(format!(
            "{operation} is missing an exact Artifact record"
        )));
    }
    if bytes > MAX_MATERIALIZED_PAGE_ARTIFACT_BYTES {
        return Err(ProtocolError::Validation(format!(
            "{operation} exceeded the bounded Artifact byte product"
        )));
    }
    Ok(())
}

fn verify_artifact_subset(
    records: &[ArtifactRecord],
    allowed: &BTreeSet<ArtifactRef>,
    operation: &str,
) -> ProtocolResult<()> {
    let mut observed = BTreeSet::new();
    let mut bytes = 0_usize;
    for record in records {
        if !allowed.contains(&record.reference) || !observed.insert(record.reference.clone()) {
            return Err(ProtocolError::Validation(format!(
                "{operation} contains a duplicate or orphan Artifact record"
            )));
        }
        verify_exact_artifact_record(record, &record.reference, operation)?;
        bytes = bytes.checked_add(record.bytes.len()).ok_or_else(|| {
            ProtocolError::Validation(format!("{operation} Artifact bytes overflowed"))
        })?;
    }
    if bytes > MAX_MATERIALIZED_PAGE_ARTIFACT_BYTES {
        return Err(ProtocolError::Validation(format!(
            "{operation} exceeded the bounded Artifact byte product"
        )));
    }
    Ok(())
}

fn validate_frontier_limits(limits: FrontierLimits) -> ProtocolResult<()> {
    for value in [
        limits.max_materialized,
        limits.max_active,
        limits.max_active_per_run,
        limits.materialize_batch,
    ] {
        let value = u64::try_from(value).map_err(|_| {
            ProtocolError::Validation("Virtual frontier limit exceeds u64".to_owned())
        })?;
        validate_positive_exact("Virtual frontier limit", value)?;
    }
    Ok(())
}

fn validate_scheduling_policy(policy: SchedulingPolicy) -> ProtocolResult<()> {
    validate_positive_exact("Virtual fairness quantum", policy.base_quantum)?;
    validate_positive_exact("Virtual priority-aging interval", policy.aging_interval)
}

fn validate_region(region: &VirtualRegion) -> ProtocolResult<()> {
    validate_identity("Virtual region", &region.region_id)?;
    validate_identity("Virtual Run", &region.run_id)?;
    validate_source_binding(&region.source)?;
    region
        .source_artifact
        .validate()
        .map_err(ProtocolError::from)?;
    validate_cursor(&region.cursor)?;
    if region
        .estimated_total
        .is_some_and(|value| value > cymule_core::MAX_EXACT_INTEGER)
    {
        return Err(ProtocolError::Validation(
            "Virtual region estimated total exceeds the exact integer range".to_owned(),
        ));
    }
    Ok(())
}

fn validate_source_binding(source: &RegionSourceBinding) -> ProtocolResult<()> {
    for (kind, value) in [
        ("RegionSource operation", source.operation.as_str()),
        ("RegionSource binding", source.binding.as_str()),
        ("RegionSource revision", source.revision.as_str()),
    ] {
        if value.is_empty() || value.chars().count() > 256 || value.chars().any(char::is_control) {
            return Err(ProtocolError::Validation(format!(
                "{kind} must contain 1..=256 printable Unicode scalar values"
            )));
        }
    }
    Ok(())
}

fn validate_archive_binding(archive: &VirtualArchiveBinding) -> ProtocolResult<()> {
    for (kind, value) in [
        ("Virtual archive binding", archive.binding.as_str()),
        ("Virtual archive revision", archive.revision.as_str()),
    ] {
        if value.is_empty() || value.chars().count() > 256 || value.chars().any(char::is_control) {
            return Err(ProtocolError::Validation(format!(
                "{kind} must contain 1..=256 printable Unicode scalar values"
            )));
        }
    }
    Ok(())
}

fn validate_cursor(cursor: &VirtualCursor) -> ProtocolResult<()> {
    validate_identity("Virtual cursor version", &cursor.version)
}

fn validate_work_item(item: &WorkItem) -> ProtocolResult<()> {
    validate_identity("Virtual work", &item.work_id)?;
    validate_identity("Virtual work region", &item.region_id)?;
    validate_identity("Virtual work Run", &item.run_id)?;
    item.payload.validate().map_err(ProtocolError::from)?;
    if let Some(capability) = &item.capability {
        validate_identity("Virtual work capability", capability)?;
    }
    validate_positive_exact("Virtual work cost", item.cost)
}

fn validate_execution_binding(reference: &ArtifactRef) -> ProtocolResult<()> {
    reference.validate().map_err(ProtocolError::from)?;
    if reference.kind != cymule_runtime::EXECUTION_BINDING_VERSION {
        return Err(ProtocolError::Validation(
            "Virtual execution binding must use cymule.execution-binding/2".to_owned(),
        ));
    }
    Ok(())
}

fn validate_migration_cardinality(
    kind: RegionMigrationKind,
    source_count: usize,
    target_count: usize,
) -> ProtocolResult<()> {
    let source_count = u64::try_from(source_count).map_err(|_| {
        ProtocolError::Validation("Virtual migration source count exceeds u64".to_owned())
    })?;
    let target_count = u64::try_from(target_count).map_err(|_| {
        ProtocolError::Validation("Virtual migration target count exceeds u64".to_owned())
    })?;
    validate_positive_exact("Virtual migration source count", source_count)?;
    validate_positive_exact("Virtual migration target count", target_count)?;
    let valid = match kind {
        RegionMigrationKind::Split => source_count == 1 && target_count >= 2,
        RegionMigrationKind::Merge => source_count >= 2 && target_count == 1,
    };
    if !valid {
        return Err(ProtocolError::Validation(
            "Virtual split requires one source and multiple targets; merge requires multiple sources and one target"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_positive_exact(kind: &str, value: u64) -> ProtocolResult<()> {
    if value == 0 || value > cymule_core::MAX_EXACT_INTEGER {
        return Err(ProtocolError::Validation(format!(
            "{kind} must use the positive exact integer range"
        )));
    }
    Ok(())
}

fn validate_exact(kind: &str, value: u64) -> ProtocolResult<()> {
    if value > cymule_core::MAX_EXACT_INTEGER {
        return Err(ProtocolError::Validation(format!(
            "{kind} exceeds the exact integer range"
        )));
    }
    Ok(())
}

fn validate_canonical_digest(kind: &str, value: &str) -> ProtocolResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProtocolError::Validation(format!(
            "{kind} digest must be lowercase SHA-256 hex"
        )));
    }
    Ok(())
}

fn virtual_compaction_certificate_id(
    certificate: &VirtualCompactionCertificate,
) -> ProtocolResult<String> {
    let mut identity = certificate.clone();
    identity.certificate_id.clear();
    cymule_core::content_id(VIRTUAL_COMPACTION_CERTIFICATE_VERSION, &identity)
        .map_err(ProtocolError::from)
}

fn virtual_compaction_command_id(command: &VirtualCompactionCommand) -> ProtocolResult<String> {
    let mut identity = command.clone();
    identity.command_id.clear();
    cymule_core::content_id(VIRTUAL_COMPACTION_CONTROL_VERSION, &identity)
        .map_err(ProtocolError::from)
}

fn verify_occurrence_proof_shape(proof: &VirtualArchiveOccurrenceProof) -> ProtocolResult<()> {
    validate_identity("Virtual archive occurrence", &proof.occurrence_id)?;
    validate_exact("Virtual archive occurrence index", proof.index)?;
    validate_exact("Virtual archive occurrence offset", proof.offset)?;
    validate_positive_exact("Virtual archive occurrence length", proof.length)?;
    validate_content_id("Virtual archive occurrence digest", &proof.digest)?;
    for step in &proof.path {
        validate_content_id("Virtual archive occurrence sibling", &step.digest)?;
    }
    Ok(())
}

struct VirtualArchiveRangeProof {
    index: u64,
    offset: u64,
    length: u64,
    digest: String,
    path: Vec<VirtualArchiveMerkleStep>,
}

fn build_virtual_archive_range_proofs<T: Serialize>(
    bytes: &[u8],
    field: &str,
    values: &BTreeMap<String, T>,
    mut leaf: impl FnMut(&str, &str) -> ProtocolResult<String>,
    node_domain: &str,
) -> ProtocolResult<Option<(String, BTreeMap<String, VirtualArchiveRangeProof>)>> {
    if values.is_empty() {
        return Ok(None);
    }
    let section = virtual_archive_top_level_value_range(bytes, field)?;
    let section_bytes = &bytes[section.clone()];
    let mut ranges = Vec::with_capacity(values.len());
    let mut leaves = Vec::with_capacity(values.len());
    for (key, value) in values {
        let value_bytes = cymule_core::canonical_bytes(value)?;
        if value_bytes.is_empty() || value_bytes.len() > MAX_VIRTUAL_ARCHIVE_BYTES {
            return Err(ProtocolError::Validation(format!(
                "Virtual archive {field} value exceeds its bounded range contract"
            )));
        }
        let mut needle = cymule_core::canonical_bytes(key)?;
        needle.push(b':');
        needle.extend_from_slice(&value_bytes);
        let key_offset =
            section.start + virtual_archive_find_unique(section_bytes, &needle, field)?;
        let offset = key_offset
            .checked_add(needle.len())
            .and_then(|value| value.checked_sub(value_bytes.len()))
            .ok_or_else(|| {
                ProtocolError::Validation(format!(
                    "Virtual archive {field} range offset overflowed"
                ))
            })?;
        let digest = format!("sha256:{}", cymule_core::sha256_bytes(&value_bytes));
        leaves.push(leaf(key, &digest)?);
        ranges.push((key.clone(), offset, value_bytes.len(), digest));
    }
    let levels = virtual_archive_merkle_levels(leaves, node_domain)?;
    let root = levels
        .last()
        .and_then(|level| level.first())
        .cloned()
        .ok_or_else(|| {
            ProtocolError::Validation(format!("Virtual archive has no {field} ranges"))
        })?;
    let (_, proof_levels) = levels.split_last().ok_or_else(|| {
        ProtocolError::Validation(format!("Virtual archive {field} tree has no levels"))
    })?;
    let mut proofs = BTreeMap::new();
    for (index, (key, offset, length, digest)) in ranges.into_iter().enumerate() {
        let mut position = index;
        let mut path = Vec::with_capacity(proof_levels.len());
        for level in proof_levels {
            let (sibling, side) = if position.is_multiple_of(2) {
                (
                    level.get(position + 1).unwrap_or(&level[position]),
                    VirtualArchiveMerkleSide::Right,
                )
            } else {
                (&level[position - 1], VirtualArchiveMerkleSide::Left)
            };
            path.push(VirtualArchiveMerkleStep {
                side,
                digest: sibling.clone(),
            });
            position /= 2;
        }
        proofs.insert(
            key,
            VirtualArchiveRangeProof {
                index: u64::try_from(index)
                    .map_err(|error| ProtocolError::Validation(error.to_string()))?,
                offset: u64::try_from(offset)
                    .map_err(|error| ProtocolError::Validation(error.to_string()))?,
                length: u64::try_from(length)
                    .map_err(|error| ProtocolError::Validation(error.to_string()))?,
                digest,
                path,
            },
        );
    }
    Ok(Some((root, proofs)))
}

fn virtual_archive_occurrence_root(
    occurrences: &BTreeMap<String, WorkOccurrence>,
) -> ProtocolResult<String> {
    let leaves = occurrences
        .iter()
        .map(|(occurrence_id, occurrence)| {
            let bytes = cymule_core::canonical_bytes(occurrence)?;
            let digest = format!("sha256:{}", cymule_core::sha256_bytes(&bytes));
            cymule_core::content_id(OCCURRENCE_LEAF_DOMAIN, &(occurrence_id, digest))
                .map_err(ProtocolError::from)
        })
        .collect::<ProtocolResult<Vec<_>>>()?;
    virtual_archive_merkle_root(leaves, OCCURRENCE_NODE_DOMAIN, "occurrence")
}

fn virtual_archive_command_root(
    journal_id: Option<&str>,
    receipts: &BTreeMap<String, VirtualPersistenceReceipt>,
) -> ProtocolResult<Option<String>> {
    if receipts.is_empty() {
        return Ok(None);
    }
    let journal_id = journal_id.ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Virtual archived receipts require their derived scheduler journal".to_owned(),
        )
    })?;
    let leaves = receipts
        .iter()
        .map(|(command_id, receipt)| {
            let bytes = cymule_core::canonical_bytes(receipt)?;
            let digest = format!("sha256:{}", cymule_core::sha256_bytes(&bytes));
            cymule_core::content_id(
                COMMAND_LEAF_DOMAIN,
                &(journal_id, command_id.as_str(), digest),
            )
            .map_err(ProtocolError::from)
        })
        .collect::<ProtocolResult<Vec<_>>>()?;
    virtual_archive_merkle_root(leaves, COMMAND_NODE_DOMAIN, "command").map(Some)
}

fn virtual_archive_merkle_root(
    level: Vec<String>,
    domain: &str,
    kind: &str,
) -> ProtocolResult<String> {
    if level.is_empty() {
        return Err(ProtocolError::Validation(format!(
            "Virtual archive has no {kind} values"
        )));
    }
    let levels = virtual_archive_merkle_levels(level, domain)?;
    levels
        .last()
        .and_then(|level| level.first())
        .cloned()
        .ok_or_else(|| ProtocolError::Validation(format!("Virtual archive has no {kind} root")))
}

fn virtual_archive_merkle_levels(
    mut level: Vec<String>,
    domain: &str,
) -> ProtocolResult<Vec<Vec<String>>> {
    let mut levels = Vec::new();
    while !level.is_empty() {
        levels.push(level.clone());
        if level.len() == 1 {
            break;
        }
        level = level
            .chunks(2)
            .map(|pair| {
                cymule_core::content_id(domain, &(&pair[0], pair.get(1).unwrap_or(&pair[0])))
                    .map_err(ProtocolError::from)
            })
            .collect::<ProtocolResult<Vec<_>>>()?;
    }
    Ok(levels)
}

fn virtual_archive_find_unique(
    haystack: &[u8],
    needle: &[u8],
    field: &str,
) -> ProtocolResult<usize> {
    let mut matches = haystack
        .windows(needle.len())
        .enumerate()
        .filter_map(|(index, candidate)| (candidate == needle).then_some(index));
    let first = matches.next().ok_or_else(|| {
        ProtocolError::IdentityMismatch(format!(
            "Virtual archive canonical {field} bytes are absent"
        ))
    })?;
    if matches.next().is_some() {
        return Err(ProtocolError::IdentityMismatch(format!(
            "Virtual archive canonical {field} bytes are ambiguous"
        )));
    }
    Ok(first)
}

fn virtual_archive_top_level_value_range(
    bytes: &[u8],
    field: &str,
) -> ProtocolResult<Range<usize>> {
    if bytes.first() != Some(&b'{') {
        return Err(ProtocolError::Encoding(
            "Virtual archive canonical bytes are not an object".to_owned(),
        ));
    }
    let mut cursor = 1;
    while cursor < bytes.len() && bytes[cursor] != b'}' {
        let key_start = cursor;
        let key_end = virtual_archive_json_string_end(bytes, key_start)?;
        let key: String = serde_json::from_slice(&bytes[key_start..key_end])
            .map_err(|error| ProtocolError::Encoding(error.to_string()))?;
        cursor = key_end;
        if bytes.get(cursor) != Some(&b':') {
            return Err(ProtocolError::Encoding(
                "Virtual archive canonical object is missing a colon".to_owned(),
            ));
        }
        cursor += 1;
        let value_start = cursor;
        let value_end = virtual_archive_json_value_end(bytes, value_start)?;
        if key == field {
            return Ok(value_start..value_end);
        }
        cursor = value_end;
        match bytes.get(cursor) {
            Some(b',') => cursor += 1,
            Some(b'}') => break,
            _ => {
                return Err(ProtocolError::Encoding(
                    "Virtual archive canonical object has an invalid separator".to_owned(),
                ));
            }
        }
    }
    Err(ProtocolError::IdentityMismatch(format!(
        "Virtual archive canonical object is missing {field}"
    )))
}

fn virtual_archive_json_string_end(bytes: &[u8], start: usize) -> ProtocolResult<usize> {
    if bytes.get(start) != Some(&b'"') {
        return Err(ProtocolError::Encoding(
            "Virtual archive canonical JSON expected a string".to_owned(),
        ));
    }
    let mut cursor = start + 1;
    let mut escaped = false;
    while let Some(byte) = bytes.get(cursor) {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            return Ok(cursor + 1);
        }
        cursor += 1;
    }
    Err(ProtocolError::Encoding(
        "Virtual archive canonical JSON has an unterminated string".to_owned(),
    ))
}

fn virtual_archive_json_value_end(bytes: &[u8], start: usize) -> ProtocolResult<usize> {
    match bytes.get(start) {
        Some(b'"') => virtual_archive_json_string_end(bytes, start),
        Some(b'{' | b'[') => {
            let open = bytes[start];
            let close = if open == b'{' { b'}' } else { b']' };
            let mut depth = 0_usize;
            let mut cursor = start;
            while cursor < bytes.len() {
                match bytes[cursor] {
                    b'"' => cursor = virtual_archive_json_string_end(bytes, cursor)?,
                    byte if byte == open => {
                        depth = depth.checked_add(1).ok_or_else(|| {
                            ProtocolError::Encoding(
                                "Virtual archive canonical JSON depth overflowed".to_owned(),
                            )
                        })?;
                        cursor += 1;
                    }
                    byte if byte == close => {
                        depth = depth.checked_sub(1).ok_or_else(|| {
                            ProtocolError::Encoding(
                                "Virtual archive canonical JSON delimiter underflowed".to_owned(),
                            )
                        })?;
                        cursor += 1;
                        if depth == 0 {
                            return Ok(cursor);
                        }
                    }
                    _ => cursor += 1,
                }
            }
            Err(ProtocolError::Encoding(
                "Virtual archive canonical JSON has an unterminated container".to_owned(),
            ))
        }
        Some(_) => {
            let mut cursor = start;
            while cursor < bytes.len() && !matches!(bytes[cursor], b',' | b'}') {
                cursor += 1;
            }
            Ok(cursor)
        }
        None => Err(ProtocolError::Encoding(
            "Virtual archive canonical JSON value is absent".to_owned(),
        )),
    }
}

fn verify_occurrence_proof(
    root_digest: &str,
    occurrence_count: u64,
    proof: &VirtualArchiveOccurrenceProof,
    occurrence: &WorkOccurrence,
) -> ProtocolResult<()> {
    verify_occurrence_proof_shape(proof)?;
    let bytes = cymule_core::canonical_bytes(occurrence)?;
    let digest = format!("sha256:{}", cymule_core::sha256_bytes(&bytes));
    if proof.occurrence_id != occurrence.occurrence_id
        || proof.index >= occurrence_count
        || proof.length != bytes.len() as u64
        || proof.digest != digest
    {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive occurrence range changed its exact value".to_owned(),
        ));
    }
    let mut current = cymule_core::content_id(
        OCCURRENCE_LEAF_DOMAIN,
        &(proof.occurrence_id.as_str(), proof.digest.as_str()),
    )?;
    let mut position = proof.index;
    let mut width = occurrence_count;
    for step in &proof.path {
        if width <= 1 {
            return Err(ProtocolError::Validation(
                "Virtual archive occurrence proof is longer than its tree".to_owned(),
            ));
        }
        let expected = if position.is_multiple_of(2) {
            VirtualArchiveMerkleSide::Right
        } else {
            VirtualArchiveMerkleSide::Left
        };
        if step.side != expected {
            return Err(ProtocolError::IdentityMismatch(
                "Virtual archive occurrence proof changed its index path".to_owned(),
            ));
        }
        current = match step.side {
            VirtualArchiveMerkleSide::Left => {
                cymule_core::content_id(OCCURRENCE_NODE_DOMAIN, &(&step.digest, &current))?
            }
            VirtualArchiveMerkleSide::Right => {
                cymule_core::content_id(OCCURRENCE_NODE_DOMAIN, &(&current, &step.digest))?
            }
        };
        position /= 2;
        width = width.div_ceil(2);
    }
    if width != 1 || current != root_digest {
        return Err(ProtocolError::IdentityMismatch(
            "Virtual archive occurrence proof does not reach its certificate root".to_owned(),
        ));
    }
    Ok(())
}

fn verify_work_proof_shape(proof: &VirtualArchiveWorkProof) -> ProtocolResult<()> {
    if proof.proof_version != "cymule.virtual-archive-work-proof/1" {
        return Err(ProtocolError::Validation(
            "unsupported Virtual archived-work proof version".to_owned(),
        ));
    }
    validate_identity("Virtual archived work", &proof.work_id)?;
    if let Some(value) = &proof.value {
        verify_archived_work_index(value)?;
        if value.work_id != proof.work_id || proof.empty_depth.is_some() {
            return Err(ProtocolError::Validation(
                "Virtual archived-work membership proof changed its work identity".to_owned(),
            ));
        }
    } else if proof.empty_depth.is_none() {
        return Err(ProtocolError::Validation(
            "Virtual archived-work absence proof requires an empty depth".to_owned(),
        ));
    }
    for sibling in &proof.siblings {
        validate_content_id("Virtual archived-work sibling", sibling)?;
    }
    Ok(())
}

fn verify_command_index_proof_shape(proof: &VirtualArchiveCommandIndexProof) -> ProtocolResult<()> {
    if proof.proof_version != "cymule.virtual-archive-command-index-proof/1" {
        return Err(ProtocolError::Validation(
            "unsupported Virtual archived-command locator proof version".to_owned(),
        ));
    }
    validate_identity("Virtual archived-command journal", &proof.journal_id)?;
    validate_identity("Virtual archived command", &proof.command_id)?;
    if let Some(value) = &proof.value {
        verify_archived_command_index(value)?;
        if value.journal_id != proof.journal_id
            || value.command_id != proof.command_id
            || proof.empty_depth.is_some()
        {
            return Err(ProtocolError::Validation(
                "Virtual archived-command locator membership changed its exact key".to_owned(),
            ));
        }
    } else if proof.empty_depth.is_none() {
        return Err(ProtocolError::Validation(
            "Virtual archived-command locator absence proof requires an empty depth".to_owned(),
        ));
    }
    for sibling in &proof.siblings {
        validate_content_id("Virtual archived-command locator sibling", sibling)?;
    }
    Ok(())
}

fn verify_archived_command_index(value: &ArchivedCommandIndex) -> ProtocolResult<()> {
    validate_identity("Virtual archived-command journal", &value.journal_id)?;
    validate_identity("Virtual archived command", &value.command_id)?;
    validate_content_id(
        "Virtual archived-command certificate",
        &value.certificate_id,
    )?;
    validate_content_id(
        "Virtual archived-command archive Resource",
        &value.archive_resource_id,
    )
}

fn verify_archived_work_index(value: &ArchivedWorkIndex) -> ProtocolResult<()> {
    validate_identity("Virtual archived work", &value.work_id)?;
    validate_identity("Virtual archived region", &value.region_id)?;
    validate_identity("Virtual archived Run", &value.run_id)?;
    validate_identity("Virtual archived occurrence", &value.occurrence_id)?;
    validate_positive_exact("Virtual archived occurrence epoch", value.max_epoch)?;
    if !matches!(
        value.terminal_state,
        WorkOccurrenceState::Succeeded
            | WorkOccurrenceState::Failed
            | WorkOccurrenceState::Cancelled
    ) {
        return Err(ProtocolError::Validation(
            "Virtual archived-work index must retain a terminal occurrence".to_owned(),
        ));
    }
    Ok(())
}

fn virtual_integrity(code: &str, message: impl Into<String>) -> ProtocolError {
    ProtocolError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn valid_sha256(value: &str) -> bool {
    cymule_core::validate_content_id("Virtual content identity", value).is_ok()
}

fn valid_content_id(value: &str) -> bool {
    valid_sha256(value)
}

fn work_index_key(work_id: &str) -> ProtocolResult<[u8; 32]> {
    validate_identity("Virtual archived work", work_id)?;
    let length = u32::try_from(work_id.len())
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let mut payload = Vec::with_capacity(4 + work_id.len());
    payload.extend_from_slice(&length.to_be_bytes());
    payload.extend_from_slice(work_id.as_bytes());
    Ok(sparse_index_raw_hash(WORK_INDEX_KEY_DOMAIN, &payload))
}

fn work_index_member_leaf(key: &[u8; 32], value: &ArchivedWorkIndex) -> ProtocolResult<String> {
    verify_archived_work_index(value)?;
    let bytes = cymule_core::canonical_bytes(value)?;
    let length =
        u32::try_from(bytes.len()).map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let mut payload = Vec::with_capacity(32 + 4 + bytes.len());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&length.to_be_bytes());
    payload.extend_from_slice(&bytes);
    Ok(sparse_index_binary_hash(
        WORK_INDEX_LEAF_DOMAIN.as_bytes(),
        &payload,
    ))
}

fn work_index_node(depth: usize, left: &str, right: &str) -> ProtocolResult<String> {
    let depth =
        u16::try_from(depth).map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let left = decode_content_id(left)?;
    let right = decode_content_id(right)?;
    let mut payload = Vec::with_capacity(2 + 64);
    payload.extend_from_slice(&depth.to_be_bytes());
    payload.extend_from_slice(&left);
    payload.extend_from_slice(&right);
    Ok(sparse_index_binary_hash(
        WORK_INDEX_NODE_DOMAIN.as_bytes(),
        &payload,
    ))
}

fn work_index_empty_hashes() -> &'static [String] {
    static HASHES: OnceLock<Vec<String>> = OnceLock::new();
    HASHES.get_or_init(|| {
        let mut hashes = vec![String::new(); WORK_INDEX_DEPTH + 1];
        hashes[WORK_INDEX_DEPTH] =
            sparse_index_binary_hash(WORK_INDEX_EMPTY_LEAF_DOMAIN.as_bytes(), &[]);
        for depth in (0..WORK_INDEX_DEPTH).rev() {
            hashes[depth] = work_index_node(depth, &hashes[depth + 1], &hashes[depth + 1])
                .expect("canonical empty archived-work children are valid");
        }
        hashes
    })
}

fn sparse_index_key_id(key: &[u8; 32]) -> String {
    let mut value = String::with_capacity("sha256:".len() + 64);
    value.push_str("sha256:");
    for byte in key {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn command_index_key(journal_id: &str, command_id: &str) -> ProtocolResult<[u8; 32]> {
    validate_identity("Virtual archived-command journal", journal_id)?;
    validate_identity("Virtual archived command", command_id)?;
    let journal_length = u32::try_from(journal_id.len())
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let command_length = u32::try_from(command_id.len())
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let mut payload = Vec::with_capacity(8 + journal_id.len() + command_id.len());
    payload.extend_from_slice(&journal_length.to_be_bytes());
    payload.extend_from_slice(journal_id.as_bytes());
    payload.extend_from_slice(&command_length.to_be_bytes());
    payload.extend_from_slice(command_id.as_bytes());
    Ok(sparse_index_raw_hash(COMMAND_INDEX_KEY_DOMAIN, &payload))
}

fn sparse_index_bit(key: &[u8; 32], depth: usize) -> bool {
    key[depth / 8] & (1 << (7 - depth % 8)) != 0
}

fn command_index_key_id(key: &[u8; 32]) -> String {
    sparse_index_key_id(key)
}

fn command_index_member_leaf(
    key: &[u8; 32],
    value: &ArchivedCommandIndex,
) -> ProtocolResult<String> {
    verify_archived_command_index(value)?;
    let bytes = cymule_core::canonical_bytes(value)?;
    let length =
        u32::try_from(bytes.len()).map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let mut payload = Vec::with_capacity(32 + 4 + bytes.len());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&length.to_be_bytes());
    payload.extend_from_slice(&bytes);
    Ok(sparse_index_binary_hash(
        COMMAND_INDEX_LEAF_DOMAIN.as_bytes(),
        &payload,
    ))
}

fn command_index_node(depth: usize, left: &str, right: &str) -> ProtocolResult<String> {
    let depth =
        u16::try_from(depth).map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let left = decode_content_id(left)?;
    let right = decode_content_id(right)?;
    let mut payload = Vec::with_capacity(2 + 64);
    payload.extend_from_slice(&depth.to_be_bytes());
    payload.extend_from_slice(&left);
    payload.extend_from_slice(&right);
    Ok(sparse_index_binary_hash(
        COMMAND_INDEX_NODE_DOMAIN.as_bytes(),
        &payload,
    ))
}

fn command_index_empty_hashes() -> &'static [String] {
    static HASHES: OnceLock<Vec<String>> = OnceLock::new();
    HASHES.get_or_init(|| {
        let mut hashes = vec![String::new(); COMMAND_INDEX_DEPTH + 1];
        hashes[COMMAND_INDEX_DEPTH] =
            sparse_index_binary_hash(COMMAND_INDEX_EMPTY_LEAF_DOMAIN.as_bytes(), &[]);
        for depth in (0..COMMAND_INDEX_DEPTH).rev() {
            hashes[depth] = command_index_node(depth, &hashes[depth + 1], &hashes[depth + 1])
                .expect("canonical empty archived-command locator children are valid");
        }
        hashes
    })
}

fn sparse_index_binary_hash(domain: &[u8], payload: &[u8]) -> String {
    let mut input = Vec::with_capacity(domain.len() + 1 + payload.len());
    input.extend_from_slice(domain);
    input.push(0);
    input.extend_from_slice(payload);
    format!("sha256:{}", cymule_core::sha256_bytes(&input))
}

fn sparse_index_raw_hash(domain: &[u8], payload: &[u8]) -> [u8; 32] {
    let mut input = Vec::with_capacity(domain.len() + 1 + payload.len());
    input.extend_from_slice(domain);
    input.push(0);
    input.extend_from_slice(payload);
    let hex = cymule_core::sha256_bytes(&input);
    let mut decoded = [0_u8; 32];
    for (index, byte) in decoded.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .expect("cymule_core::sha256_bytes returns lowercase SHA-256 hex");
    }
    decoded
}

fn decode_content_id(value: &str) -> ProtocolResult<[u8; 32]> {
    validate_content_id("Virtual sparse-index node", value)?;
    let hex = &value["sha256:".len()..];
    let mut decoded = [0_u8; 32];
    for (index, byte) in decoded.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    }
    Ok(decoded)
}

fn validate_content_id(kind: &str, value: &str) -> ProtocolResult<()> {
    cymule_core::validate_content_id(kind, value).map_err(ProtocolError::from)
}

fn validate_identity(kind: &str, value: &str) -> ProtocolResult<()> {
    cymule_core::validate_identity(kind, value)
        .map_err(|error| ProtocolError::Validation(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_current_keys_preserve_exact_family_and_scheduler_authority() {
        let scheduler = "scheduler:semantic-keys";
        let identity = "semantic:one";
        let keys = [
            (
                VirtualStateFamily::Regions,
                virtual_region_key(scheduler, identity),
            ),
            (
                VirtualStateFamily::Work,
                virtual_work_key(scheduler, identity),
            ),
            (
                VirtualStateFamily::Occurrences,
                virtual_occurrence_key(scheduler, identity),
            ),
            (
                VirtualStateFamily::Runs,
                virtual_run_key(scheduler, identity),
            ),
        ];
        let mut unique = BTreeSet::new();
        for (family, key) in keys {
            let key = key.expect("semantic current key derives");
            assert_eq!(
                key,
                virtual_state_storage_key(scheduler, family, identity).unwrap()
            );
            assert_ne!(
                key,
                virtual_state_storage_key("scheduler:another", family, identity).unwrap()
            );
            assert!(unique.insert(key));
        }
        assert!(virtual_region_key("", identity).is_err());
        assert!(virtual_work_key(scheduler, "").is_err());
        assert!(virtual_occurrence_key(scheduler, "invalid\nidentity").is_err());
        assert!(virtual_run_key(scheduler, &"x".repeat(513)).is_err());
    }

    #[test]
    fn certificate_current_key_preserves_exact_family_and_scheduler_authority() {
        let scheduler = "scheduler:certificate-key";
        let certificate = cymule_core::content_id("test.virtual-certificate/1", &"one").unwrap();
        let key = virtual_certificate_key(scheduler, &certificate).unwrap();
        assert_eq!(
            key,
            virtual_state_storage_key(scheduler, VirtualStateFamily::Certificates, &certificate)
                .unwrap(),
        );
        assert_ne!(
            key,
            virtual_certificate_key("scheduler:another", &certificate).unwrap(),
        );
        for family in [
            VirtualStateFamily::Regions,
            VirtualStateFamily::Work,
            VirtualStateFamily::Occurrences,
            VirtualStateFamily::Runs,
        ] {
            assert_ne!(
                key,
                virtual_state_storage_key(scheduler, family, &certificate).unwrap(),
            );
        }
        assert!(virtual_certificate_key("", &certificate).is_err());
        assert!(virtual_certificate_key(scheduler, "").is_err());
        assert!(virtual_certificate_key(scheduler, "invalid\nidentity").is_err());
    }

    fn active_region_page(
        current: &VirtualCurrent,
        after_storage_key: Option<String>,
        storage_keys: Vec<String>,
        has_more: bool,
    ) -> VirtualActiveRegionPage {
        VirtualActiveRegionPage::from_authenticated_range(
            current.body.roots.active_regions.clone(),
            current.body.counts.active_regions,
            after_storage_key,
            storage_keys,
            has_more,
        )
        .expect("authenticated active-region page seals")
    }

    fn state_reads(leaves: Vec<VirtualStateLeaf>) -> Vec<VirtualStateRead> {
        leaves
            .into_iter()
            .map(|leaf| {
                VirtualStateRead::new(
                    leaf.family(),
                    leaf.storage_key().expect("test leaf key derives"),
                    Some(leaf),
                )
                .expect("test membership read seals")
            })
            .collect()
    }

    fn prepare_from_state(
        command: &VirtualPersistenceCommand,
        current: Option<&VirtualCurrent>,
        leaves: Vec<VirtualStateLeaf>,
        operation: &VirtualOperationAuthority,
    ) -> VirtualReduction {
        try_prepare_from_state(command, current, leaves, operation)
            .expect("test preparation succeeds")
    }

    fn try_prepare_from_state(
        command: &VirtualPersistenceCommand,
        current: Option<&VirtualCurrent>,
        leaves: Vec<VirtualStateLeaf>,
        operation: &VirtualOperationAuthority,
    ) -> ProtocolResult<VirtualReduction> {
        let state = leaves
            .into_iter()
            .map(|leaf| {
                let key = (
                    leaf.family(),
                    leaf.storage_key().expect("test leaf key derives"),
                );
                (key, leaf)
            })
            .collect::<BTreeMap<_, _>>();
        let mut reads = Vec::new();
        let mut loaded = BTreeSet::new();
        loop {
            let source = VirtualKeyedSource::from_reads(
                command.scheduler_id(),
                current.cloned(),
                reads.clone(),
            )?;
            let authority = VirtualReductionAuthority::new(source, operation.clone());
            match prepare_virtual(command, &authority) {
                Ok(reduction) => return Ok(reduction),
                Err(VirtualPreparationError::ReadRequired {
                    family,
                    storage_key,
                }) => {
                    assert!(
                        loaded.insert((family, storage_key.clone())),
                        "preparation repeated one exact read requirement"
                    );
                    reads.push(
                        VirtualStateRead::new(
                            family,
                            storage_key.clone(),
                            state.get(&(family, storage_key)).cloned(),
                        )
                        .expect("test exact read seals"),
                    );
                }
                Err(VirtualPreparationError::Protocol(error)) => return Err(error),
            }
        }
    }

    fn test_artifact(kind: &str, bytes: Vec<u8>) -> ArtifactRecord {
        let reference = cymule_core::artifact_ref(kind, &bytes).expect("Artifact identity derives");
        ArtifactRecord { reference, bytes }
    }

    fn initialization_postcondition() -> (VirtualPersistenceCommand, VirtualPostcondition) {
        let source = test_artifact("test.virtual-source/1", b"source".to_vec());
        let command = VirtualPersistenceCommand::new(VirtualPersistenceOperation::Initialize(
            VirtualInitializationCommand {
                control_version: VIRTUAL_INITIALIZATION_CONTROL_VERSION.to_owned(),
                scheduler_id: "scheduler:test".to_owned(),
                command_id: "initialize:test".to_owned(),
                limits: FrontierLimits {
                    max_materialized: 8,
                    max_active: 4,
                    max_active_per_run: 2,
                    materialize_batch: 4,
                },
                scheduling_policy: SchedulingPolicy::default(),
                archive: VirtualArchiveBinding {
                    binding: "archive:test".to_owned(),
                    revision: "revision:test".to_owned(),
                },
                regions: vec![VirtualRegion {
                    region_id: "region:test".to_owned(),
                    run_id: "run:test".to_owned(),
                    source: RegionSourceBinding {
                        operation: "test.source".to_owned(),
                        binding: "source:test".to_owned(),
                        revision: "revision:test".to_owned(),
                    },
                    source_artifact: source.reference.clone(),
                    cursor: VirtualCursor {
                        version: "cursor:test".to_owned(),
                        position: "start".to_owned(),
                        exhausted: false,
                    },
                    estimated_total: None,
                }],
                runs: vec![VirtualRunDefinition {
                    run_id: "run:test".to_owned(),
                    execution: VirtualRunExecution::Direct {
                        plan_id: cymule_core::content_id("test.plan/1", &"plan:test")
                            .expect("test Plan identity derives"),
                    },
                }],
                source_artifacts: vec![source],
            },
        ))
        .expect("initialization command seals");
        let reduction = prepare_from_state(
            &command,
            None,
            Vec::new(),
            &VirtualOperationAuthority::Initialize,
        );
        let node = cymule_core::content_id("test.virtual-map-node/1", &())
            .expect("test StateRoot node identity derives");
        let postcondition = reduction
            .finish(VirtualStateRoots {
                regions: virtual_state_root_id(VirtualStateFamily::Regions, Some(&node), 1)
                    .expect("region root seals"),
                active_regions: virtual_state_root_id(
                    VirtualStateFamily::ActiveRegions,
                    Some(&node),
                    1,
                )
                .expect("active-region root seals"),
                parked: virtual_state_root_id(VirtualStateFamily::Parked, None, 0)
                    .expect("parked root seals"),
                parked_index: virtual_state_root_id(VirtualStateFamily::ParkedIndex, None, 0)
                    .expect("parked-index root seals"),
                work: virtual_state_root_id(VirtualStateFamily::Work, None, 0)
                    .expect("work root seals"),
                occurrences: virtual_state_root_id(VirtualStateFamily::Occurrences, None, 0)
                    .expect("occurrence root seals"),
                runs: virtual_state_root_id(VirtualStateFamily::Runs, Some(&node), 1)
                    .expect("Run root seals"),
                migrations: virtual_state_root_id(VirtualStateFamily::Migrations, None, 0)
                    .expect("migration root seals"),
                certificates: virtual_state_root_id(VirtualStateFamily::Certificates, None, 0)
                    .expect("certificate root seals"),
            })
            .expect("initialization postcondition seals");
        (command, postcondition)
    }

    fn claim_outcome_plan() -> cymule_core::SealedPlan {
        cymule_core::seal_plan(cymule_core::PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "virtual-claim-outcome".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![cymule_core::Definition {
                id: "main".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: cymule_core::Region {
                    steps: Vec::new(),
                    result: cymule_core::Expression::Literal {
                        value: serde_json::json!(null),
                    },
                },
            }],
            metadata: BTreeMap::new(),
        })
        .expect("claim outcome Plan seals")
    }

    fn claim_outcome_clock() -> ClockObservation {
        let source_generation =
            cymule_core::content_id("test.virtual-clock-generation/1", &"claim-outcome")
                .expect("Clock generation identifies");
        let logical_time = 41;
        ClockObservation {
            clock_version: cymule_durable_protocol::CLOCK_OBSERVATION_VERSION.to_owned(),
            observation_id: cymule_durable_protocol::clock_observation_id(
                "clock:claim-outcome",
                &source_generation,
                "slot:claim-outcome",
                logical_time,
                logical_time,
            )
            .expect("Clock observation identifies"),
            source_id: "clock:claim-outcome".to_owned(),
            source_generation,
            scope: "slot:claim-outcome".to_owned(),
            logical_time,
            observed_unix_ms: logical_time,
        }
    }

    fn claim_outcome_fixture(
        claimed: bool,
    ) -> (VirtualPersistenceReceipt, cymule_core::SealedPlan) {
        let plan = claim_outcome_plan();
        let execution_binding = cymule_core::artifact_ref(
            cymule_runtime::EXECUTION_BINDING_VERSION,
            b"claim-outcome-binding",
        )
        .expect("claim binding identifies");
        let clock_observation = claim_outcome_clock();
        let command = VirtualClaimCommand {
            control_version: VIRTUAL_CLAIM_CONTROL_VERSION.to_owned(),
            command_id: "claim:outcome".to_owned(),
            owner: "worker:claim-outcome".to_owned(),
            slot_id: clock_observation.scope.clone(),
            capabilities: BTreeSet::new(),
            execution_binding: execution_binding.clone(),
            clock: clock_observation.reference(),
            lease_ttl: 5,
        };
        let persistence = VirtualPersistenceCommand::new(VirtualPersistenceOperation::Claim(
            VirtualClaimPersistenceCommand {
                scheduler_id: "scheduler:claim-outcome".to_owned(),
                command: command.clone(),
            },
        ))
        .expect("claim persistence command seals");
        let claim = claimed.then(|| {
            let item = WorkItem {
                work_id: "work:claim-outcome".to_owned(),
                region_id: "region:claim-outcome".to_owned(),
                run_id: "run:claim-outcome".to_owned(),
                payload: cymule_core::artifact_ref("test.virtual-payload/1", b"payload")
                    .expect("work payload identifies"),
                capability: None,
                priority: 0,
                cost: 1,
            };
            let epoch = 1;
            ClaimedWork {
                occurrence_id: cymule_core::content_id(
                    VIRTUAL_WORK_OCCURRENCE_VERSION,
                    &(item.work_id.as_str(), epoch),
                )
                .expect("claim occurrence identifies"),
                item,
                owner: command.owner.clone(),
                epoch,
                plan_id: plan.plan_id.clone(),
                execution_binding: execution_binding.clone(),
                lease: VirtualClaimLease {
                    resource: command.slot_id.clone(),
                    owner: command.owner.clone(),
                    epoch: 1,
                    expires_at: clock_observation.logical_time + command.lease_ttl,
                    clock: command.clock.clone(),
                },
            }
        });
        let claim_receipt = VirtualClaimReceipt {
            command,
            clock_observation,
            run_execution: claim.as_ref().map(|_| VirtualRunExecution::Direct {
                plan_id: plan.plan_id.clone(),
            }),
            claim,
            evolution_selection: None,
        };
        let receipt = VirtualPersistenceReceipt::new(
            persistence,
            Some(
                cymule_core::content_id("test.virtual-parent-current/1", &"claim-outcome")
                    .expect("parent current identifies"),
            ),
            VirtualPersistenceEvidence::None,
            VirtualMutationSet::new(Vec::new()).expect("empty mutation set seals"),
            cymule_core::content_id("test.virtual-result-body/1", &claimed)
                .expect("result body identifies"),
            VirtualPersistenceOutcome::Claimed(claim_receipt),
        )
        .expect("claim persistence receipt seals");
        (receipt, plan)
    }

    struct ClaimPreparationFixture {
        current: VirtualCurrent,
        command: VirtualPersistenceCommand,
        operation: VirtualOperationAuthority,
        leaves: [VirtualStateLeaf; 3],
    }

    fn claim_preparation_current(
        initialized: &VirtualCurrent,
        work: &VirtualWorkCurrent,
    ) -> VirtualCurrent {
        let mut frontier = initialized.body.frontier.clone();
        frontier.ready.insert(
            work.item.run_id.clone(),
            VecDeque::from([work.item.clone()]),
        );
        frontier.ready_since.insert(work.item.work_id.clone(), 0);
        let mut counts = initialized.body.counts;
        counts.hot_work = 1;
        let mut roots = initialized.body.roots.clone();
        let node = cymule_core::content_id("test.virtual-claim-work-root/1", work)
            .expect("claim work root identifies");
        roots.work = virtual_state_root_id(VirtualStateFamily::Work, Some(&node), 1)
            .expect("claim work root seals");
        VirtualCurrent::new(
            VirtualCurrentBody::new(draft_from_current(initialized, frontier, counts), roots)
                .expect("claim source body seals"),
            initialized.last_receipt_id.clone(),
        )
        .expect("claim source current seals")
    }

    fn claim_preparation_fixture() -> ClaimPreparationFixture {
        let (_, initialized) = initialization_postcondition();
        let plan = claim_outcome_plan();
        let binding = cymule_runtime::ExecutionBinding {
            version: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
            context: cymule_runtime::BindingContextDescriptor {
                version: cymule_runtime::RUNTIME_COMPOSITION_VERSION.to_owned(),
                providers: Vec::new(),
            },
            components: BTreeMap::new(),
            effects: BTreeMap::new(),
        };
        let binding = test_artifact(
            cymule_runtime::EXECUTION_BINDING_VERSION,
            binding.canonical_bytes().expect("claim binding seals"),
        );
        let mut region = initialized
            .receipt
            .mutations
            .operations
            .iter()
            .find_map(|mutation| match mutation.after_leaf() {
                Some(VirtualStateLeaf::Regions(region)) => Some(region),
                _ => None,
            })
            .expect("initialized region is retained");
        region.hot_work_count = 1;
        let work = VirtualWorkCurrent {
            leaf_version: VIRTUAL_WORK_CURRENT_VERSION.to_owned(),
            scheduler_id: initialized.current.body.scheduler_id.clone(),
            item: WorkItem {
                work_id: "work:claim-preparation".to_owned(),
                region_id: region.region.region_id.clone(),
                run_id: region.region.run_id.clone(),
                payload: test_artifact("test.virtual-payload/1", b"payload".to_vec()).reference,
                capability: None,
                priority: 0,
                cost: 1,
            },
            max_epoch: 0,
            latest_occurrence_id: None,
            placement: VirtualWorkPlacement::Ready,
        };
        let run = VirtualRunCurrent {
            leaf_version: VIRTUAL_RUN_CURRENT_VERSION.to_owned(),
            scheduler_id: work.scheduler_id.clone(),
            run_id: work.item.run_id.clone(),
            execution: VirtualRunExecution::Direct {
                plan_id: plan.plan_id.clone(),
            },
            weight: 1,
            deficit: 0,
        };
        let current = claim_preparation_current(&initialized.current, &work);
        let clock = claim_outcome_clock();
        let command = VirtualPersistenceCommand::new(VirtualPersistenceOperation::Claim(
            VirtualClaimPersistenceCommand {
                scheduler_id: work.scheduler_id.clone(),
                command: VirtualClaimCommand {
                    control_version: VIRTUAL_CLAIM_CONTROL_VERSION.to_owned(),
                    command_id: "claim:preparation".to_owned(),
                    owner: "worker:claim-preparation".to_owned(),
                    slot_id: clock.scope.clone(),
                    execution_binding: binding.reference.clone(),
                    capabilities: BTreeSet::new(),
                    clock: clock.reference(),
                    lease_ttl: 5,
                },
            },
        ))
        .expect("claim command seals");
        let operation = VirtualOperationAuthority::Claim {
            lease: VirtualClaimLease {
                resource: clock.scope.clone(),
                owner: "worker:claim-preparation".to_owned(),
                epoch: 1,
                expires_at: clock.logical_time + 5,
                clock: clock.reference(),
            },
            clock,
            execution: VirtualExecutionAuthority::new(run.run_id.clone(), plan, binding)
                .expect("claim execution authority seals"),
            evolution_selection: None,
        };
        ClaimPreparationFixture {
            current,
            command,
            operation,
            leaves: [
                VirtualStateLeaf::Runs(run),
                VirtualStateLeaf::Work(work),
                VirtualStateLeaf::Regions(region),
            ],
        }
    }

    #[test]
    fn claim_preparation_preview_requires_complete_run_reads() {
        let fixture = claim_preparation_fixture();
        let VirtualPersistenceOperation::Claim(command) = &fixture.command.operation else {
            panic!("fixture must contain a claim command");
        };
        let run = &fixture.leaves[0];
        let run_key = run.storage_key().unwrap();
        let source = VirtualKeyedSource::from_reads(
            fixture.command.scheduler_id(),
            Some(fixture.current.clone()),
            Vec::new(),
        )
        .unwrap();
        assert!(matches!(
            preview_virtual_claim(command, &source),
            Err(VirtualPreparationError::ReadRequired { family, storage_key })
                if family == VirtualStateFamily::Runs && storage_key == run_key
        ));
        let absent = VirtualKeyedSource::from_reads(
            fixture.command.scheduler_id(),
            Some(fixture.current.clone()),
            vec![VirtualStateRead::new(VirtualStateFamily::Runs, run_key, None).unwrap()],
        )
        .unwrap();
        assert!(matches!(
            preview_virtual_claim(command, &absent),
            Err(VirtualPreparationError::Protocol(
                ProtocolError::IllegalTransition(_)
            ))
        ));
        let complete = VirtualKeyedSource::from_reads(
            fixture.command.scheduler_id(),
            Some(fixture.current),
            state_reads(vec![run.clone()]),
        )
        .unwrap();
        let preview = preview_virtual_claim(command, &complete)
            .expect("complete Run reads permit fairness selection")
            .expect("one ready item is eligible");
        let VirtualStateLeaf::Work(work) = &fixture.leaves[1] else {
            panic!("fixture must contain selected work");
        };
        assert_eq!(preview.item, work.item);
    }

    #[test]
    fn claim_preparation_extends_reads_through_work_region_and_occurrence_absence() {
        let fixture = claim_preparation_fixture();
        let reduction = prepare_from_state(
            &fixture.command,
            Some(&fixture.current),
            fixture.leaves.to_vec(),
            &fixture.operation,
        );
        let VirtualPersistenceOutcome::Claimed(receipt) = &reduction.outcome else {
            panic!("claim preparation must produce a claim receipt");
        };
        let claim = receipt.claim.as_ref().expect("ready work is claimed");
        assert_eq!(claim.item.work_id, "work:claim-preparation");
        assert_eq!(claim.epoch, 1);
        assert_eq!(
            reduction.current.frontier.active[&claim.item.work_id],
            *claim
        );
        assert!(reduction.current.frontier.ready.is_empty());
        assert!(reduction.mutations.operations.iter().any(|mutation| {
            matches!(
                mutation,
                VirtualStateMutation::Occurrences { before: None, after: Some(occurrence) }
                    if occurrence.occurrence.occurrence_id == claim.occurrence_id
            )
        }));
        assert_eq!(
            reduction,
            prepare_from_state(
                &fixture.command,
                Some(&fixture.current),
                fixture.leaves.to_vec(),
                &fixture.operation,
            )
        );
    }

    #[test]
    fn claim_preparation_rejects_orphan_leaves_and_absence_reads() {
        let fixture = claim_preparation_fixture();
        let VirtualPersistenceOperation::Claim(command) = &fixture.command.operation else {
            panic!("fixture must contain a claim command");
        };
        let reduction = prepare_from_state(
            &fixture.command,
            Some(&fixture.current),
            fixture.leaves.to_vec(),
            &fixture.operation,
        );
        let VirtualPersistenceOutcome::Claimed(receipt) = &reduction.outcome else {
            panic!("claim preparation must produce a claim receipt");
        };
        let claim = receipt.claim.as_ref().unwrap();
        let mut reads = state_reads(fixture.leaves.to_vec());
        reads.push(
            VirtualStateRead::new(
                VirtualStateFamily::Occurrences,
                virtual_occurrence_key(command.scheduler_id.as_str(), &claim.occurrence_id)
                    .unwrap(),
                None,
            )
            .unwrap(),
        );
        let exact = VirtualKeyedSource::from_reads(
            &command.scheduler_id,
            Some(fixture.current.clone()),
            reads.clone(),
        )
        .unwrap();
        assert!(matches!(
            preview_virtual_claim(command, &exact),
            Err(VirtualPreparationError::Protocol(
                ProtocolError::IllegalTransition(_)
            ))
        ));
        assert_eq!(
            prepare_virtual(
                &fixture.command,
                &VirtualReductionAuthority::new(exact, fixture.operation.clone()),
            )
            .expect("the final claim accepts exactly its complete source"),
            reduction,
        );
        let VirtualStateLeaf::Work(mut orphan_work) = fixture.leaves[1].clone() else {
            panic!("fixture must contain selected work");
        };
        orphan_work.item.work_id = "work:orphan".to_owned();
        let VirtualStateLeaf::Regions(mut orphan_region) = fixture.leaves[2].clone() else {
            panic!("fixture must contain a selected region");
        };
        orphan_region.region.region_id = "region:orphan".to_owned();
        let mut orphans = state_reads(vec![
            VirtualStateLeaf::Work(orphan_work),
            VirtualStateLeaf::Regions(orphan_region),
            VirtualStateLeaf::ActiveRegions(VirtualActiveRegionCurrent {
                leaf_version: VIRTUAL_ACTIVE_REGION_CURRENT_VERSION.to_owned(),
                scheduler_id: command.scheduler_id.clone(),
                region_id: claim.item.region_id.clone(),
            }),
        ]);
        for family in [VirtualStateFamily::Work, VirtualStateFamily::ActiveRegions] {
            orphans.push(
                VirtualStateRead::new(
                    family,
                    virtual_state_storage_key(&command.scheduler_id, family, "absent:orphan")
                        .unwrap(),
                    None,
                )
                .unwrap(),
            );
        }
        for orphan in orphans {
            let mut extra_reads = reads.clone();
            extra_reads.push(orphan);
            let source = VirtualKeyedSource::from_reads(
                &command.scheduler_id,
                Some(fixture.current.clone()),
                extra_reads,
            )
            .unwrap();
            assert!(matches!(
                prepare_virtual(
                    &fixture.command,
                    &VirtualReductionAuthority::new(source, fixture.operation.clone()),
                ),
                Err(VirtualPreparationError::Protocol(
                    ProtocolError::IllegalTransition(_)
                ))
            ));
        }
    }

    #[test]
    fn park_reason_rejects_unknown_members() {
        let error = serde_json::from_value::<ParkReason>(serde_json::json!({
            "kind": "wait",
            "key": "wait:one",
            "unexpected": true
        }))
        .expect_err("closed park reason must reject unknown members");
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn wait_park_reason_requires_an_exact_m1_wait_identity() {
        assert!(
            verify_park_reason(&ParkReason::Wait {
                key: "wait:not-content-addressed".to_owned(),
            })
            .is_err()
        );
        let wait_id = cymule_core::content_id("test.virtual-wait/1", &"exact")
            .expect("test Wait identity derives");
        verify_park_reason(&ParkReason::Wait { key: wait_id })
            .expect("an exact M1 Wait identity is admitted");
    }

    #[test]
    fn wait_capacity_directory_is_a_required_current_member() {
        let current = initialization_postcondition().1.current;
        let mut wire = serde_json::to_value(current).expect("Virtual current serializes");
        wire["body"]["frontier"]
            .as_object_mut()
            .expect("Virtual frontier is an object")
            .remove("wait_activations");
        assert!(serde_json::from_value::<VirtualCurrent>(wire).is_err());
    }

    fn current_with_wait_capacities(
        capacities: BTreeMap<String, VirtualWaitActivationCapacity>,
        parked: u64,
    ) -> ProtocolResult<VirtualCurrent> {
        let (_, initialized) = initialization_postcondition();
        let mut frontier = initialized.current.body.frontier.clone();
        frontier.wait_activations = capacities;
        let mut counts = initialized.current.body.counts;
        counts.parked = parked;
        counts.hot_work = parked;
        let mut draft = draft_from_current(&initialized.current, frontier, counts);
        draft.limits.max_materialized = MAX_VIRTUAL_CURRENT_FRONTIER_ITEMS;
        VirtualCurrent::new(
            VirtualCurrentBody::new(draft, initialized.current.body.roots.clone())?,
            initialized.current.last_receipt_id,
        )
    }

    #[test]
    fn wait_capacity_rejects_the_first_aggregate_above_the_exact_item_bound() {
        let wait_a = cymule_core::content_id("test.virtual-wait/1", &"a")
            .expect("first Wait identity derives");
        let wait_b = cymule_core::content_id("test.virtual-wait/1", &"b")
            .expect("second Wait identity derives");
        let wait_c = cymule_core::content_id("test.virtual-wait/1", &"c")
            .expect("third Wait identity derives");
        let capacity = |work_items| VirtualWaitActivationCapacity {
            work_items,
            index_pages: work_items.div_ceil(MAX_VIRTUAL_PARKED_INDEX_PAGE_ITEMS as u64),
            source_bytes: 1,
            mutation_bytes: 1,
        };
        let exact = BTreeMap::from([(wait_a, capacity(1_024)), (wait_b, capacity(1_020))]);
        let exact_items = exact
            .values()
            .map(|capacity| capacity.source_items().unwrap())
            .sum::<u64>();
        assert_eq!(exact_items, 4_096);
        current_with_wait_capacities(exact.clone(), 2_044)
            .expect("the largest reachable aggregate below the 4097-item bound seals");

        let mut oversized = exact;
        oversized.insert(wait_c, capacity(1));
        assert_eq!(
            oversized
                .values()
                .map(|capacity| capacity.source_items().unwrap())
                .sum::<u64>(),
            4_099
        );
        let error = current_with_wait_capacities(oversized, 2_045)
            .expect_err("the first reachable aggregate above 4097 items must fail before CAS");
        assert!(
            error
                .to_string()
                .contains("aggregate source or mutation bound")
        );
    }

    #[test]
    fn wait_capacity_source_and_mutation_bytes_have_exact_terminal_boundaries() {
        let wait_id = cymule_core::content_id("test.virtual-wait/1", &"byte-boundary")
            .expect("Wait identity derives");
        let mut frontier = initialization_postcondition().1.current.body.frontier;
        let operation_count = 3_u64;
        let empty_mutation_bytes = virtual_mutation_set_encoded_bytes(0, 0).unwrap();
        let exact_operation_bytes =
            MAX_VIRTUAL_MUTATION_BYTES as u64 - (empty_mutation_bytes - 2) - (operation_count - 1);
        frontier.wait_activations.insert(
            wait_id.clone(),
            VirtualWaitActivationCapacity {
                work_items: 1,
                index_pages: 1,
                source_bytes: MAX_VIRTUAL_REDUCTION_SOURCE_BYTES as u64 - 1_024,
                mutation_bytes: exact_operation_bytes,
            },
        );
        verify_virtual_wait_activation_source_budget(1_024, &frontier)
            .expect("the exact source-byte ceiling is admitted");
        assert_eq!(
            virtual_mutation_set_encoded_bytes(operation_count, exact_operation_bytes).unwrap(),
            MAX_VIRTUAL_MUTATION_BYTES as u64
        );

        frontier
            .wait_activations
            .get_mut(&wait_id)
            .unwrap()
            .source_bytes += 1;
        assert!(verify_virtual_wait_activation_source_budget(1_024, &frontier).is_err());
        let capacity = frontier.wait_activations.get_mut(&wait_id).unwrap();
        capacity.source_bytes -= 1;
        capacity.mutation_bytes += 1;
        let (_, mutation_items, _, mutation_bytes) =
            virtual_wait_activation_totals(&frontier).unwrap();
        assert!(
            virtual_mutation_set_encoded_bytes(mutation_items, mutation_bytes).unwrap()
                > MAX_VIRTUAL_MUTATION_BYTES as u64
        );
    }

    fn single_wait_activation_leaves(
        scheduler_id: &str,
        wait_id: &str,
    ) -> (ParkReason, Vec<VirtualStateLeaf>) {
        let payload = test_artifact("test.virtual-payload/1", b"payload".to_vec()).reference;
        let item = WorkItem {
            work_id: "work:wait-capacity".to_owned(),
            region_id: "region:test".to_owned(),
            run_id: "run:test".to_owned(),
            payload,
            capability: None,
            priority: 0,
            cost: 1,
        };
        let occurrence_id = cymule_core::content_id(
            VIRTUAL_WORK_OCCURRENCE_VERSION,
            &(item.work_id.as_str(), 1_u64),
        )
        .expect("parked occurrence identity derives");
        let work = VirtualWorkCurrent {
            leaf_version: VIRTUAL_WORK_CURRENT_VERSION.to_owned(),
            scheduler_id: scheduler_id.to_owned(),
            item: item.clone(),
            max_epoch: 1,
            latest_occurrence_id: Some(occurrence_id),
            placement: VirtualWorkPlacement::Parked,
        };
        let reason = ParkReason::Wait {
            key: wait_id.to_owned(),
        };
        let parked = VirtualParkedCurrent {
            leaf_version: VIRTUAL_PARKED_CURRENT_VERSION.to_owned(),
            scheduler_id: scheduler_id.to_owned(),
            parked: ParkedWork {
                item: item.clone(),
                reason: reason.clone(),
            },
        };
        let page = VirtualParkedIndexPage {
            page_version: VIRTUAL_PARKED_INDEX_PAGE_VERSION.to_owned(),
            scheduler_id: scheduler_id.to_owned(),
            reason: reason.clone(),
            page: 0,
            work_ids: BTreeSet::from([item.work_id.clone()]),
            next_page: None,
        };
        (
            reason,
            vec![
                VirtualStateLeaf::ParkedIndex(page),
                VirtualStateLeaf::Parked(parked),
                VirtualStateLeaf::Work(work),
            ],
        )
    }

    fn wait_activation_currents(
        initialized: &VirtualCurrent,
        wait_id: &str,
        reason: &ParkReason,
        leaves: &[VirtualStateLeaf],
    ) -> (VirtualCurrent, VirtualCurrent) {
        let scheduler_id = &initialized.body.scheduler_id;
        let source_without_current =
            VirtualKeyedSource::from_reads(scheduler_id, None, state_reads(leaves.to_vec()))
                .expect("exact Wait leaves form a bounded source");
        let pages = exact_parked_index_pages(&source_without_current, reason)
            .expect("the exact Wait page chain resolves");
        let capacity =
            recompute_virtual_wait_activation_capacity(wait_id, &pages, &source_without_current)
                .expect("exact Wait capacity recomputes");

        let mut frontier = initialized.body.frontier.clone();
        frontier
            .wait_activations
            .insert(wait_id.to_owned(), capacity);
        let mut counts = initialized.body.counts;
        counts.parked = 1;
        counts.hot_work = 1;
        counts.hot_occurrences = 1;
        let node = cymule_core::content_id("test.virtual-wait-root/1", &"node")
            .expect("Wait root node derives");
        let mut roots = initialized.body.roots.clone();
        roots.parked = virtual_state_root_id(VirtualStateFamily::Parked, Some(&node), 1).unwrap();
        roots.parked_index =
            virtual_state_root_id(VirtualStateFamily::ParkedIndex, Some(&node), 1).unwrap();
        roots.work = virtual_state_root_id(VirtualStateFamily::Work, Some(&node), 1).unwrap();
        roots.occurrences =
            virtual_state_root_id(VirtualStateFamily::Occurrences, Some(&node), 1).unwrap();
        let current = VirtualCurrent::new(
            VirtualCurrentBody::new(draft_from_current(initialized, frontier, counts), roots)
                .expect("Wait source body seals"),
            initialized.last_receipt_id.clone(),
        )
        .expect("Wait source current seals");
        let mut tampered_frontier = current.body.frontier.clone();
        tampered_frontier
            .wait_activations
            .get_mut(wait_id)
            .expect("selected Wait capacity exists")
            .source_bytes += 1;
        let tampered_current = VirtualCurrent::new(
            VirtualCurrentBody::new(
                draft_from_current(&current, tampered_frontier, current.body.counts),
                current.body.roots.clone(),
            )
            .expect("shape-valid tampered Wait body seals"),
            current.last_receipt_id.clone(),
        )
        .expect("shape-valid tampered Wait current seals");
        (current, tampered_current)
    }

    fn maximum_wait_activation_authority(
        scheduler_id: &str,
        wait_id: String,
    ) -> (VirtualPersistenceCommand, VirtualOperationAuthority) {
        let result = test_artifact(
            cymule_durable_protocol::WAIT_RESULT_ARTIFACT_KIND,
            b"result".to_vec(),
        );
        let mut applied_wait_ids = (0_u64
            ..(cymule_durable_protocol::MAX_WAIT_DELIVERY_TARGETS - 1) as u64)
            .map(|index| cymule_core::content_id("test.virtual-unrelated-wait/1", &index).unwrap())
            .collect::<BTreeSet<_>>();
        applied_wait_ids.insert(wait_id);
        assert_eq!(
            applied_wait_ids.len(),
            cymule_durable_protocol::MAX_WAIT_DELIVERY_TARGETS
        );
        let activation = cymule_durable_protocol::WaitActivation::new(
            "activation:wait-capacity",
            cymule_durable_protocol::WaitActivationSource::Signal {
                key: "signal:wait-capacity".to_owned(),
            },
            applied_wait_ids.clone(),
            result.reference.clone(),
        )
        .expect("maximum-width M1 activation seals");
        let receipt = WaitActivationReceipt {
            receipt_version: cymule_durable_protocol::WAIT_ACTIVATION_RECEIPT_VERSION.to_owned(),
            activation,
            applied_wait_ids,
            ready_run_ids: BTreeSet::new(),
        };
        receipt.verify().expect("maximum-width M1 receipt verifies");
        let activation =
            VirtualActivationCommand::new(scheduler_id, receipt.activation.activation_id.clone())
                .expect("Virtual activation command seals");
        let command =
            VirtualPersistenceCommand::new(VirtualPersistenceOperation::ActivateWait(activation))
                .expect("Virtual activation persistence command seals");
        let operation = VirtualOperationAuthority::ActivateWait { receipt, result };
        (command, operation)
    }

    struct WaitActivationCapacityFixture {
        current: VirtualCurrent,
        tampered_current: VirtualCurrent,
        leaves: Vec<VirtualStateLeaf>,
        command: VirtualPersistenceCommand,
        operation: VirtualOperationAuthority,
    }

    fn wait_activation_capacity_fixture() -> WaitActivationCapacityFixture {
        let initialized = initialization_postcondition().1.current;
        let scheduler_id = initialized.body.scheduler_id.clone();
        let wait_id = cymule_core::content_id("test.virtual-wait/1", &"selected")
            .expect("selected Wait identity derives");
        let (reason, leaves) = single_wait_activation_leaves(&scheduler_id, &wait_id);
        let (current, tampered_current) =
            wait_activation_currents(&initialized, &wait_id, &reason, &leaves);
        let (command, operation) = maximum_wait_activation_authority(&scheduler_id, wait_id);
        WaitActivationCapacityFixture {
            current,
            tampered_current,
            leaves,
            command,
            operation,
        }
    }

    #[test]
    fn park_transition_capacity_matches_exact_future_activation_leaves() {
        let initialized = initialization_postcondition().1.current;
        let scheduler_id = initialized.body.scheduler_id.clone();
        let wait_id = cymule_core::content_id("test.virtual-wait/1", &"park-update")
            .expect("park-update Wait identity derives");
        let (reason, leaves) = single_wait_activation_leaves(&scheduler_id, &wait_id);
        let VirtualStateLeaf::ParkedIndex(page) = &leaves[0] else {
            panic!("fixture requires one parked-index page")
        };
        let VirtualStateLeaf::Parked(parked) = &leaves[1] else {
            panic!("fixture requires one parked leaf")
        };
        let VirtualStateLeaf::Work(work) = &leaves[2] else {
            panic!("fixture requires one work leaf")
        };
        let mut frontier = initialized.body.frontier;
        update_virtual_wait_activation_capacity(
            &mut frontier,
            &wait_id,
            work,
            parked,
            &[VirtualStateMutation::ParkedIndex {
                before: None,
                after: Some(page.clone()),
            }],
        )
        .expect("park transition computes a bounded Wait capacity");
        let source = VirtualKeyedSource::from_reads(scheduler_id, None, state_reads(leaves))
            .expect("future activation leaves form an exact source");
        let pages = exact_parked_index_pages(&source, &reason).unwrap();
        let expected = recompute_virtual_wait_activation_capacity(&wait_id, &pages, &source)
            .expect("future activation charge recomputes");
        assert_eq!(frontier.wait_activations.get(&wait_id), Some(&expected));
    }

    #[test]
    fn activation_ignores_unrelated_m1_waits_and_consumes_its_exact_capacity() {
        let fixture = wait_activation_capacity_fixture();
        let source = VirtualKeyedSource::from_reads(
            fixture.current.body.scheduler_id.clone(),
            Some(fixture.current.clone()),
            state_reads(fixture.leaves.clone()),
        )
        .expect("activation loads only the exact indexed Wait leaves");
        let reduction = prepare_virtual(
            &fixture.command,
            &VirtualReductionAuthority::new(source, fixture.operation.clone()),
        )
        .expect("unrelated M1 targets add no negative-read burden");
        assert!(reduction.current.frontier.wait_activations.is_empty());
        assert_eq!(reduction.mutations.operations.len(), 3);
        assert!(matches!(
            reduction.outcome,
            VirtualPersistenceOutcome::Activated { woken: 1, .. }
        ));

        let tampered = VirtualKeyedSource::from_reads(
            fixture.current.body.scheduler_id,
            Some(fixture.tampered_current),
            state_reads(fixture.leaves),
        )
        .expect("tampered capacity current remains shape-valid");
        assert!(matches!(
            prepare_virtual(
                &fixture.command,
                &VirtualReductionAuthority::new(tampered, fixture.operation),
            ),
            Err(VirtualPreparationError::Protocol(
                ProtocolError::IdentityMismatch(_)
            ))
        ));
    }

    #[test]
    fn archive_retirement_command_is_content_derived() {
        let certificate_id = cymule_core::content_id("test.virtual-certificate/1", &"one")
            .expect("certificate identity derives");
        let command =
            VirtualArchiveRetirementCommand::new(certificate_id).expect("retirement command seals");
        command.verify().expect("sealed command verifies");

        let mut tampered = command;
        tampered.certificate_id = cymule_core::content_id("test.virtual-certificate/1", &"two")
            .expect("second certificate identity derives");
        assert!(matches!(
            tampered.verify(),
            Err(ProtocolError::IdentityMismatch(_))
        ));
    }

    #[test]
    fn archive_index_resolvers_preserve_missing_object_category() {
        let work = ArchivedWorkIndex {
            work_id: "work:missing-node".to_owned(),
            region_id: "region:missing-node".to_owned(),
            run_id: "run:missing-node".to_owned(),
            occurrence_id: "occurrence:missing-node".to_owned(),
            max_epoch: 1,
            terminal_state: WorkOccurrenceState::Succeeded,
        };
        let work_root = virtual_work_index_empty_root();
        let absence = resolve_virtual_work_index_proof(&work_root, &work.work_id, |_| Ok(None))
            .expect("empty work index proves absence");
        let (work_update, _) = build_virtual_work_index_update(&work_root, absence, &work)
            .expect("work insertion derives");
        assert!(matches!(
            resolve_virtual_work_index_proof(
                &work_update.result_root_digest,
                &work.work_id,
                |_| Ok(None),
            ),
            Err(ProtocolError::NotFound { message })
                if message.contains("archived-work index node")
        ));

        let command = ArchivedCommandIndex {
            journal_id: "journal:missing-node".to_owned(),
            command_id: "command:missing-node".to_owned(),
            certificate_id: cymule_core::content_id("test.virtual-certificate/1", &())
                .expect("certificate identity derives"),
            archive_resource_id: cymule_core::content_id("test.virtual-resource/1", &())
                .expect("archive Resource identity derives"),
        };
        let command_root = virtual_command_index_empty_root();
        let absence = resolve_virtual_command_index_proof(
            &command_root,
            &command.journal_id,
            &command.command_id,
            |_| Ok(None),
        )
        .expect("empty command index proves absence");
        let (command_update, _) =
            build_virtual_command_index_update(&command_root, absence, &command)
                .expect("command insertion derives");
        assert!(matches!(
            resolve_virtual_command_index_proof(
                &command_update.result_root_digest,
                &command.journal_id,
                &command.command_id,
                |_| Ok(None),
            ),
            Err(ProtocolError::NotFound { message })
                if message.contains("archived-command locator node")
        ));
    }

    #[test]
    fn virtual_control_reads_and_commits_require_nullable_revision_members() {
        let (command, postcondition) = initialization_postcondition();
        let revision = cymule_core::content_id("test.virtual-state-root/1", &())
            .expect("physical revision derives");
        let query = VirtualCurrentQuery {
            scheduler_id: command.scheduler_id().to_owned(),
            expected_revision: None,
        };
        query.verify().expect("unpinned query verifies");
        let mut query_wire = serde_json::to_value(&query).expect("query serializes");
        assert_eq!(
            query_wire.get("expected_revision"),
            Some(&serde_json::Value::Null)
        );
        query_wire
            .as_object_mut()
            .expect("query is an object")
            .remove("expected_revision");
        assert!(serde_json::from_value::<VirtualCurrentQuery>(query_wire).is_err());

        let replay = VirtualCommit {
            observed_revision: revision.clone(),
            committed_revision: None,
            receipt: postcondition.receipt.clone(),
        };
        replay
            .verify_for(&command)
            .expect("exact lost-ack replay verifies");
        let mut replay_wire = serde_json::to_value(&replay).expect("commit serializes");
        assert_eq!(
            replay_wire.get("committed_revision"),
            Some(&serde_json::Value::Null)
        );
        replay_wire
            .as_object_mut()
            .expect("commit is an object")
            .remove("committed_revision");
        assert!(serde_json::from_value::<VirtualCommit>(replay_wire).is_err());

        VirtualCommit {
            observed_revision: revision.clone(),
            committed_revision: Some(revision),
            receipt: postcondition.receipt,
        }
        .verify_for(&command)
        .expect("new commit returns the exact resulting revision");
    }

    #[test]
    fn virtual_claim_outcome_carries_a_plan_only_for_an_actual_claim() {
        let (empty_receipt, _) = claim_outcome_fixture(false);
        let empty = VirtualClaimOutcome::no_work(empty_receipt.clone())
            .expect("empty receipt projects to NoWork");
        empty.verify().expect("NoWork verifies");
        assert!(matches!(empty, VirtualClaimOutcome::NoWork { .. }));
        assert!(
            VirtualClaimOutcome::claimed(empty_receipt, claim_outcome_fixture(true).1).is_err()
        );

        let (claimed_receipt, plan) = claim_outcome_fixture(true);
        let claimed = VirtualClaimOutcome::claimed(claimed_receipt.clone(), plan.clone())
            .expect("actual claim projects with its exact Plan");
        claimed.verify().expect("claimed outcome verifies");
        let VirtualClaimOutcome::Claimed {
            claim,
            plan: returned_plan,
            ..
        } = &claimed
        else {
            panic!("actual claim returned NoWork")
        };
        assert_eq!(claim.plan_id, plan.plan_id);
        assert_eq!(returned_plan.as_ref(), &plan);
        assert!(VirtualClaimOutcome::no_work(claimed_receipt).is_err());

        let mut no_work_wire = serde_json::to_value(
            VirtualClaimOutcome::no_work(claim_outcome_fixture(false).0).unwrap(),
        )
        .unwrap();
        no_work_wire
            .as_object_mut()
            .unwrap()
            .insert("plan".to_owned(), serde_json::to_value(&plan).unwrap());
        assert!(serde_json::from_value::<VirtualClaimOutcome>(no_work_wire).is_err());

        let claimed_wire = serde_json::to_value(claimed).unwrap();
        for member in ["claim", "plan"] {
            let mut missing = claimed_wire.clone();
            missing.as_object_mut().unwrap().remove(member);
            assert!(serde_json::from_value::<VirtualClaimOutcome>(missing).is_err());
        }
    }

    #[test]
    fn virtual_claim_outcome_rejects_plan_claim_and_binding_substitution() {
        let (receipt, plan) = claim_outcome_fixture(true);
        let VirtualPersistenceOutcome::Claimed(retained_claim) = &receipt.outcome else {
            panic!("fixture returned a non-claim persistence outcome")
        };
        let mut missing_execution = retained_claim.clone();
        missing_execution.run_execution = None;
        assert!(
            verify_virtual_claim_receipt(&receipt.command.persistence_id, &missing_execution,)
                .is_err()
        );
        let mut changed_clock = retained_claim.clone();
        changed_clock.clock_observation.logical_time += 1;
        assert!(
            verify_virtual_claim_receipt(&receipt.command.persistence_id, &changed_clock).is_err()
        );

        let mut wrong_candidate = plan.candidate.clone();
        wrong_candidate.name = "virtual-claim-other-plan".to_owned();
        let wrong_plan = cymule_core::seal_plan(wrong_candidate).expect("other Plan seals");
        assert!(VirtualClaimOutcome::claimed(receipt.clone(), wrong_plan).is_err());

        let mut outcome = VirtualClaimOutcome::claimed(receipt, plan).unwrap();
        let VirtualClaimOutcome::Claimed { claim, .. } = &mut outcome else {
            panic!("fixture returned NoWork")
        };
        claim.execution_binding = cymule_core::artifact_ref(
            cymule_runtime::EXECUTION_BINDING_VERSION,
            b"substituted-binding",
        )
        .unwrap();
        assert!(matches!(
            outcome.verify(),
            Err(ProtocolError::IdentityMismatch(_))
        ));
    }

    #[test]
    fn normalized_leaf_surface_matches_mutation_keys_and_rejects_bad_sources() {
        let (_, postcondition) = initialization_postcondition();
        let leaves = postcondition
            .receipt
            .mutations
            .operations
            .iter()
            .map(|mutation| {
                let leaf = mutation
                    .after_leaf()
                    .expect("initialization inserts leaves");
                assert!(mutation.before_leaf().is_none());
                assert_eq!(leaf.family(), mutation.family());
                assert_eq!(
                    leaf.storage_key().expect("leaf key derives"),
                    mutation.storage_key().expect("mutation key derives")
                );
                leaf
            })
            .collect::<Vec<_>>();
        VirtualKeyedSource::from_reads(
            &postcondition.current.body.scheduler_id,
            Some(postcondition.current.clone()),
            state_reads(leaves.clone()),
        )
        .expect("typed reads rebuild one exact keyed source");

        let duplicate = leaves[0].clone();
        assert!(matches!(
            VirtualKeyedSource::from_reads(
                "scheduler:test",
                None,
                state_reads(vec![duplicate.clone(), duplicate]),
            ),
            Err(ProtocolError::IllegalTransition(_))
        ));

        let mut foreign = leaves[0].clone();
        match &mut foreign {
            VirtualStateLeaf::Regions(leaf) => leaf.scheduler_id = "scheduler:foreign".to_owned(),
            _ => panic!("initialization emits the region family first"),
        }
        assert!(matches!(
            VirtualKeyedSource::from_reads(
                "scheduler:test",
                Some(postcondition.current),
                state_reads(vec![foreign]),
            ),
            Err(ProtocolError::IdentityMismatch(_))
        ));
    }

    #[test]
    fn preparation_distinguishes_unread_absence_and_rejects_orphan_reads() {
        let (command, postcondition) = initialization_postcondition();
        let source = VirtualKeyedSource::from_reads(command.scheduler_id(), None, Vec::new())
            .expect("empty pinned genesis view seals");
        let authority =
            VirtualReductionAuthority::new(source, VirtualOperationAuthority::Initialize);
        assert!(matches!(
            prepare_virtual(&command, &authority),
            Err(VirtualPreparationError::ReadRequired { .. })
        ));

        let mut reads = postcondition
            .receipt
            .mutations
            .operations
            .iter()
            .map(|mutation| {
                VirtualStateRead::new(
                    mutation.family(),
                    mutation.storage_key().expect("mutation key derives"),
                    None,
                )
                .expect("genesis absence read seals")
            })
            .collect::<Vec<_>>();
        let orphan_key = virtual_state_storage_key(
            command.scheduler_id(),
            VirtualStateFamily::Work,
            "work:orphan-read",
        )
        .expect("orphan test key derives");
        reads.push(
            VirtualStateRead::new(VirtualStateFamily::Work, orphan_key, None)
                .expect("orphan absence read seals"),
        );
        let source = VirtualKeyedSource::from_reads(command.scheduler_id(), None, reads)
            .expect("bounded source with one orphan read seals");
        let authority =
            VirtualReductionAuthority::new(source, VirtualOperationAuthority::Initialize);
        assert!(matches!(
            prepare_virtual(&command, &authority),
            Err(VirtualPreparationError::Protocol(
                ProtocolError::IllegalTransition(_)
            ))
        ));
    }

    struct LargeHistoryMaterializationFixture {
        current: VirtualCurrent,
        leaves: Vec<VirtualStateLeaf>,
        region: VirtualRegion,
        command: VirtualPersistenceCommand,
        preflight_source: VirtualKeyedSource,
        selection: VirtualActiveRegionSelectionProof,
    }

    fn large_history_current(postcondition: &VirtualPostcondition) -> VirtualCurrent {
        let mut roots = postcondition.current.body.roots.clone();
        let node = cymule_core::content_id("test.virtual-large-region-root/1", &())
            .expect("large region root derives");
        roots.regions = virtual_state_root_id(
            VirtualStateFamily::Regions,
            Some(&node),
            cymule_core::MAX_EXACT_INTEGER,
        )
        .expect("large cumulative region root seals");
        let mut counts = postcondition.current.body.counts;
        counts.regions = cymule_core::MAX_EXACT_INTEGER;
        let body = VirtualCurrentBody::new(
            VirtualCurrentDraft {
                scheduler_id: postcondition.current.body.scheduler_id.clone(),
                limits: postcondition.current.body.limits,
                scheduling_policy: postcondition.current.body.scheduling_policy,
                archive: postcondition.current.body.archive.clone(),
                frontier: postcondition.current.body.frontier.clone(),
                archived_work_index_root_digest: postcondition
                    .current
                    .body
                    .archived_work_index_root_digest
                    .clone(),
                archived_command_index_root_digest: postcondition
                    .current
                    .body
                    .archived_command_index_root_digest
                    .clone(),
                counts,
            },
            roots,
        )
        .expect("large-history current body seals");
        VirtualCurrent::new(body, postcondition.current.last_receipt_id.clone())
            .expect("large-history current seals")
    }

    fn large_history_materialization_fixture() -> LargeHistoryMaterializationFixture {
        let (initialization, postcondition) = initialization_postcondition();
        let VirtualPersistenceOperation::Initialize(initialize) = &initialization.operation else {
            panic!("fixture must be an initialization command");
        };
        let region = initialize.regions[0].clone();
        let current = large_history_current(&postcondition);
        let leaves = postcondition
            .receipt
            .mutations
            .operations
            .iter()
            .filter(|mutation| {
                matches!(
                    mutation.family(),
                    VirtualStateFamily::Regions | VirtualStateFamily::ActiveRegions
                )
            })
            .map(|mutation| {
                mutation
                    .after_leaf()
                    .expect("genesis inserts selected leaves")
            })
            .collect::<Vec<_>>();
        let active_key = leaves
            .iter()
            .find(|leaf| leaf.family() == VirtualStateFamily::ActiveRegions)
            .expect("active-region leaf is present")
            .storage_key()
            .expect("active-region leaf key derives");
        let command = VirtualPersistenceCommand::new(VirtualPersistenceOperation::Materialize(
            VirtualMaterializationCommand {
                control_version: VIRTUAL_MATERIALIZATION_CONTROL_VERSION.to_owned(),
                scheduler_id: initialize.scheduler_id.clone(),
                command_id: "materialize:large-history".to_owned(),
                region_id: region.region_id.clone(),
                expected_source: region.source.clone(),
                expected_cursor: region.cursor.clone(),
            },
        ))
        .expect("materialization command seals");
        let preflight_source = VirtualKeyedSource::from_reads(
            &initialize.scheduler_id,
            Some(current.clone()),
            state_reads(leaves.clone()),
        )
        .expect("provider preflight source seals");
        let selection = VirtualActiveRegionSelectionProof::from_authenticated_pages(
            &current,
            &active_region_page(&current, None, vec![active_key.clone()], false),
            None,
        )
        .expect("authenticated selection seals");
        LargeHistoryMaterializationFixture {
            current,
            leaves,
            region,
            command,
            preflight_source,
            selection,
        }
    }

    #[test]
    fn active_region_family_bounds_selection_after_large_retired_history() {
        let fixture = large_history_materialization_fixture();
        assert_eq!(fixture.leaves.len(), 2);
        preflight_virtual_provider(
            &fixture.command,
            &fixture.preflight_source,
            Some(&fixture.selection),
        )
        .expect("provider preflight needs only the selected active-region leaves");
        let wrong_key = virtual_active_region_key(
            fixture.command.scheduler_id(),
            "region:unverified-selection",
        )
        .expect("wrong active-region key derives");
        let wrong_selection = VirtualActiveRegionSelectionProof::from_authenticated_pages(
            &fixture.current,
            &active_region_page(&fixture.current, None, vec![wrong_key], false),
            None,
        )
        .expect("typed wrong selection witness seals before command binding");
        assert!(matches!(
            preflight_virtual_provider(
                &fixture.command,
                &fixture.preflight_source,
                Some(&wrong_selection),
            ),
            Err(VirtualPreparationError::Protocol(
                ProtocolError::IdentityMismatch(_)
            ))
        ));
        assert!(matches!(
            try_prepare_from_state(
                &fixture.command,
                Some(&fixture.current),
                fixture.leaves.clone(),
                &VirtualOperationAuthority::Materialize {
                    selection: wrong_selection,
                    page: MaterializedPage {
                        items: Vec::new(),
                        artifacts: Vec::new(),
                        next_cursor: VirtualCursor {
                            version: fixture.region.cursor.version.clone(),
                            position: "finished".to_owned(),
                            exhausted: true,
                        },
                    },
                    archived_work_proofs: BTreeMap::new(),
                },
            ),
            Err(ProtocolError::IdentityMismatch(_))
        ));
        let reduction = prepare_from_state(
            &fixture.command,
            Some(&fixture.current),
            fixture.leaves,
            &VirtualOperationAuthority::Materialize {
                selection: fixture.selection,
                page: MaterializedPage {
                    items: Vec::new(),
                    artifacts: Vec::new(),
                    next_cursor: VirtualCursor {
                        version: fixture.region.cursor.version,
                        position: "finished".to_owned(),
                        exhausted: true,
                    },
                },
                archived_work_proofs: BTreeMap::new(),
            },
        );
        assert_eq!(
            reduction.current.counts.regions,
            cymule_core::MAX_EXACT_INTEGER
        );
        assert_eq!(reduction.current.counts.active_regions, 0);
        assert!(reduction.mutations.operations.iter().any(|mutation| {
            matches!(
                mutation,
                VirtualStateMutation::ActiveRegions {
                    before: Some(_),
                    after: None
                }
            )
        }));
    }

    fn two_active_region_current(last_region: String) -> VirtualCurrent {
        let (_, postcondition) = initialization_postcondition();
        let base = postcondition.current;
        let node = cymule_core::content_id("test.virtual-two-active-root/1", &())
            .expect("two-entry root node derives");
        let mut roots = base.body.roots.clone();
        roots.regions = virtual_state_root_id(VirtualStateFamily::Regions, Some(&node), 2)
            .expect("two-region root seals");
        roots.active_regions =
            virtual_state_root_id(VirtualStateFamily::ActiveRegions, Some(&node), 2)
                .expect("two-active-region root seals");
        let mut counts = base.body.counts;
        counts.regions = 2;
        counts.active_regions = 2;
        let mut frontier = base.body.frontier.clone();
        frontier.last_region = Some(last_region);
        let body = VirtualCurrentBody::new(
            VirtualCurrentDraft {
                scheduler_id: base.body.scheduler_id.clone(),
                limits: base.body.limits,
                scheduling_policy: base.body.scheduling_policy,
                archive: base.body.archive.clone(),
                frontier,
                archived_work_index_root_digest: base.body.archived_work_index_root_digest.clone(),
                archived_command_index_root_digest: base
                    .body
                    .archived_command_index_root_digest
                    .clone(),
                counts,
            },
            roots,
        )
        .expect("cursor-bearing current body seals");
        VirtualCurrent::new(body, base.last_receipt_id).expect("cursor-bearing current seals")
    }

    #[test]
    fn active_region_selection_authenticates_one_suffix_and_at_most_one_wrap() {
        let current = two_active_region_current("region:last".to_owned());
        let after = virtual_active_region_key(&current.body.scheduler_id, "region:last")
            .expect("retained cursor key derives");
        let selected = virtual_active_region_key(&current.body.scheduler_id, "region:first")
            .expect("wrapped head key derives");
        let empty_suffix = active_region_page(&current, Some(after.clone()), Vec::new(), false);

        let wrong_root = cymule_core::content_id("test.virtual-wrong-active-root/1", &())
            .expect("wrong active root derives");
        let wrong_root_page = VirtualActiveRegionPage::from_authenticated_range(
            wrong_root,
            current.body.counts.active_regions,
            Some(after.clone()),
            Vec::new(),
            false,
        )
        .expect("independently authenticated wrong-root page seals");
        assert!(matches!(
            VirtualActiveRegionSelectionProof::from_authenticated_pages(
                &current,
                &wrong_root_page,
                None,
            ),
            Err(ProtocolError::Integrity { code, .. })
                if code == "virtual_active_region_page_root_mismatch"
        ));
        assert!(matches!(
            VirtualActiveRegionSelectionProof::from_authenticated_pages(
                &current,
                &active_region_page(&current, None, Vec::new(), false),
                None,
            ),
            Err(ProtocolError::Integrity { code, .. })
                if code == "virtual_active_region_page_cursor_mismatch"
        ));
        assert!(
            VirtualActiveRegionPage::from_authenticated_range(
                current.body.roots.active_regions.clone(),
                current.body.counts.active_regions,
                Some(after.clone()),
                vec![selected.clone(), selected.clone()],
                false,
            )
            .is_err()
        );

        assert!(matches!(
            VirtualActiveRegionSelectionProof::from_authenticated_pages(
                &current,
                &empty_suffix,
                None,
            ),
            Err(ProtocolError::Integrity { code, .. })
                if code == "virtual_active_region_wrap_missing"
        ));

        let proof = VirtualActiveRegionSelectionProof::from_authenticated_pages(
            &current,
            &empty_suffix,
            Some(&active_region_page(
                &current,
                None,
                vec![selected.clone()],
                true,
            )),
        )
        .expect("one authenticated head wrap selects the first region");
        assert_eq!(proof.authenticated_page_count(), 2);
        assert_eq!(proof.selected_storage_key(), Some(selected.as_str()));

        let successor = active_region_page(&current, Some(after), vec![selected], false);
        assert!(matches!(
            VirtualActiveRegionSelectionProof::from_authenticated_pages(
                &current,
                &successor,
                Some(&active_region_page(&current, None, Vec::new(), false)),
            ),
            Err(ProtocolError::IllegalTransition(_))
        ));
    }

    #[test]
    fn active_region_selection_accepts_the_exact_identity_byte_ceiling() {
        let maximal_region = char::from_u32(0x10_ffff)
            .expect("maximum Unicode scalar is valid")
            .to_string()
            .repeat(512);
        let current = two_active_region_current(maximal_region.clone());
        let after = virtual_active_region_key(&current.body.scheduler_id, &maximal_region)
            .expect("maximum region cursor key derives");
        let selected = virtual_active_region_key(&current.body.scheduler_id, "region:first")
            .expect("wrapped key derives");
        let suffix = active_region_page(&current, Some(after), Vec::new(), false);
        let wrapped = active_region_page(&current, None, vec![selected], true);
        VirtualActiveRegionSelectionProof::from_authenticated_pages(
            &current,
            &suffix,
            Some(&wrapped),
        )
        .expect("exact worst-case identity byte ceiling is admitted");
    }

    struct MigrationFixture {
        postcondition: VirtualPostcondition,
        persistence: VirtualPersistenceCommand,
        operation: VirtualOperationAuthority,
        leaves: Vec<VirtualStateLeaf>,
    }

    fn migration_targets(
        source_region: &VirtualRegion,
        target_a: &ArtifactRecord,
        target_b: &ArtifactRecord,
    ) -> Vec<VirtualRegion> {
        [
            ("region:target-a", target_a.reference.clone()),
            ("region:target-b", target_b.reference.clone()),
        ]
        .into_iter()
        .map(|(region_id, source_artifact)| VirtualRegion {
            region_id: region_id.to_owned(),
            run_id: source_region.run_id.clone(),
            source: source_region.source.clone(),
            source_artifact,
            cursor: VirtualCursor {
                version: source_region.cursor.version.clone(),
                position: "target-start".to_owned(),
                exhausted: false,
            },
            estimated_total: None,
        })
        .collect()
    }

    fn migration_fixture() -> MigrationFixture {
        let (initialization, postcondition) = initialization_postcondition();
        let VirtualPersistenceOperation::Initialize(initialize) = &initialization.operation else {
            panic!("fixture must be an initialization command");
        };
        let source_region = initialize.regions[0].clone();
        let coverage = test_artifact("test.virtual-migration-coverage/1", b"coverage".to_vec());
        let target_a = test_artifact("test.virtual-source/1", b"target-a".to_vec());
        let target_b = test_artifact("test.virtual-source/1", b"target-b".to_vec());
        let migration_id = "migration:active-regions".to_owned();
        let command_id = "migrate:active-regions".to_owned();
        let migration_binding = "migrator:test".to_owned();
        let migration_revision = "revision:test".to_owned();
        let targets = migration_targets(&source_region, &target_a, &target_b);
        let request = RegionMigrationRequest {
            migration_id: migration_id.clone(),
            kind: RegionMigrationKind::Split,
            source_region_ids: BTreeSet::from([source_region.region_id.clone()]),
            target_count: targets.len(),
            migration_binding: migration_binding.clone(),
            migration_revision: migration_revision.clone(),
        };
        let migration = RegionMigrationCommand {
            control_version: VIRTUAL_REGION_MIGRATION_CONTROL_VERSION.to_owned(),
            command_id: command_id.clone(),
            plan: RegionMigrationPlan {
                migration_version: VIRTUAL_REGION_MIGRATION_VERSION.to_owned(),
                migration_id,
                kind: RegionMigrationKind::Split,
                expected_sources: BTreeMap::from([(
                    source_region.region_id.clone(),
                    RegionSourceCheckpoint {
                        source: source_region.source.clone(),
                        cursor: source_region.cursor,
                    },
                )]),
                targets,
                migration_binding,
                migration_revision,
                coverage_evidence: coverage.reference.clone(),
            },
        };
        let persistence = VirtualPersistenceCommand::new(
            VirtualPersistenceOperation::MigrateRegion(VirtualMigrationPersistenceCommand {
                scheduler_id: initialize.scheduler_id.clone(),
                command_id,
                request,
            }),
        )
        .expect("migration intent seals");
        let operation = VirtualOperationAuthority::MigrateRegion {
            command: migration,
            coverage_evidence: coverage,
            target_source_artifacts: vec![target_a, target_b],
        };
        let leaves = postcondition
            .receipt
            .mutations
            .operations
            .iter()
            .filter(|mutation| {
                matches!(
                    mutation.family(),
                    VirtualStateFamily::Regions | VirtualStateFamily::ActiveRegions
                )
            })
            .map(|mutation| {
                mutation
                    .after_leaf()
                    .expect("genesis inserts selected leaves")
            })
            .collect::<Vec<_>>();
        MigrationFixture {
            postcondition,
            persistence,
            operation,
            leaves,
        }
    }

    fn migration_proposal(fixture: &MigrationFixture) -> VirtualRegionMigrationProposal {
        let VirtualOperationAuthority::MigrateRegion {
            command,
            coverage_evidence,
            target_source_artifacts,
        } = &fixture.operation
        else {
            panic!("fixture must contain migration authority");
        };
        VirtualRegionMigrationProposal {
            plan: command.plan.clone(),
            coverage_evidence: coverage_evidence.clone(),
            target_source_artifacts: target_source_artifacts.clone(),
        }
    }

    #[test]
    fn migration_proposal_retains_complete_material_in_the_exact_reduction() {
        let fixture = migration_fixture();
        let VirtualPersistenceOperation::MigrateRegion(command) = &fixture.persistence.operation
        else {
            panic!("fixture must contain migration intent");
        };
        let proposal = migration_proposal(&fixture);
        proposal
            .verify_for(command)
            .expect("complete provider proposal matches its semantic request");
        let mut expected_artifacts = vec![proposal.coverage_evidence.clone()];
        expected_artifacts.extend(proposal.target_source_artifacts.iter().cloned());
        let authority = proposal
            .into_authority(command)
            .expect("complete proposal derives the closed migration authority");
        assert_eq!(authority, fixture.operation);
        let reduction = prepare_from_state(
            &fixture.persistence,
            Some(&fixture.postcondition.current),
            fixture.leaves.clone(),
            &authority,
        );
        assert_eq!(reduction.artifacts, expected_artifacts);
        let VirtualPersistenceEvidence::Migrated {
            command: retained,
            coverage_evidence,
            target_source_artifacts,
        } = reduction.evidence
        else {
            panic!("migration reduction must retain its existing evidence shape");
        };
        assert_eq!(retained.command_id, command.command_id);
        assert_eq!(coverage_evidence, expected_artifacts[0]);
        assert_eq!(target_source_artifacts, expected_artifacts[1..]);

        let mut shared_source = migration_proposal(&fixture);
        shared_source.plan.targets[1].source_artifact =
            shared_source.plan.targets[0].source_artifact.clone();
        shared_source.target_source_artifacts.truncate(1);
        shared_source
            .verify_for(command)
            .expect("shared target references require one exact distinct Artifact record");
    }

    #[test]
    fn migration_proposal_rejects_missing_extra_duplicate_and_changed_material() {
        let fixture = migration_fixture();
        let VirtualPersistenceOperation::MigrateRegion(command) = &fixture.persistence.operation
        else {
            panic!("fixture must contain migration intent");
        };
        let proposal = migration_proposal(&fixture);
        let mut invalid = Vec::new();
        let mut missing = proposal.clone();
        missing.target_source_artifacts.pop();
        invalid.push(missing);
        let mut extra = proposal.clone();
        extra.target_source_artifacts.push(test_artifact(
            "test.virtual-source/1",
            b"orphan-source".to_vec(),
        ));
        invalid.push(extra);
        let mut duplicate = proposal.clone();
        duplicate
            .target_source_artifacts
            .push(duplicate.target_source_artifacts[0].clone());
        invalid.push(duplicate);
        let mut repeated_coverage = proposal.clone();
        repeated_coverage.coverage_evidence = repeated_coverage.target_source_artifacts[0].clone();
        repeated_coverage.plan.coverage_evidence =
            repeated_coverage.coverage_evidence.reference.clone();
        invalid.push(repeated_coverage);
        let mut changed_coverage = proposal.clone();
        changed_coverage.coverage_evidence.bytes.push(0);
        invalid.push(changed_coverage);
        let mut changed_source = proposal.clone();
        changed_source.target_source_artifacts[0].bytes.push(0);
        invalid.push(changed_source);
        let mut changed_generation = proposal;
        changed_generation.plan.migration_revision = "revision:other".to_owned();
        invalid.push(changed_generation);
        for proposal in invalid {
            assert!(matches!(
                proposal.verify_for(command),
                Err(ProtocolError::Validation(_) | ProtocolError::IdentityMismatch(_))
            ));
            assert!(matches!(
                proposal.into_authority(command),
                Err(ProtocolError::Validation(_) | ProtocolError::IdentityMismatch(_))
            ));
        }
    }

    #[test]
    fn migration_proposal_bounds_coverage_and_target_bytes_together() {
        let fixture = migration_fixture();
        let VirtualPersistenceOperation::MigrateRegion(command) = &fixture.persistence.operation
        else {
            panic!("fixture must contain migration intent");
        };
        let mut proposal = migration_proposal(&fixture);
        let target_bytes = proposal
            .target_source_artifacts
            .iter()
            .map(|record| record.bytes.len())
            .sum::<usize>();
        proposal.coverage_evidence = test_artifact(
            "test.virtual-migration-coverage/1",
            vec![0; MAX_MATERIALIZED_PAGE_ARTIFACT_BYTES - target_bytes],
        );
        proposal.plan.coverage_evidence = proposal.coverage_evidence.reference.clone();
        proposal
            .verify_for(command)
            .expect("the exact aggregate Artifact byte ceiling is admitted");
        proposal.coverage_evidence.bytes.push(0);
        proposal.coverage_evidence.reference = cymule_core::artifact_ref(
            &proposal.coverage_evidence.reference.kind,
            &proposal.coverage_evidence.bytes,
        )
        .unwrap();
        proposal.plan.coverage_evidence = proposal.coverage_evidence.reference.clone();
        assert!(proposal.coverage_evidence.bytes.len() < MAX_MATERIALIZED_PAGE_ARTIFACT_BYTES);
        assert!(target_bytes < MAX_MATERIALIZED_PAGE_ARTIFACT_BYTES);
        assert!(matches!(
            proposal.verify_for(command),
            Err(ProtocolError::Validation(message))
                if message == "Virtual migration exceeded the aggregate Artifact byte product"
        ));
        assert!(matches!(
            proposal.into_authority(command),
            Err(ProtocolError::Validation(_))
        ));
    }

    #[test]
    fn migration_requires_and_replays_exact_active_region_mutations() {
        let fixture = migration_fixture();
        let region_only = fixture
            .leaves
            .iter()
            .filter(|leaf| leaf.family() == VirtualStateFamily::Regions)
            .cloned()
            .collect();
        let missing_active = VirtualKeyedSource::from_reads(
            fixture.persistence.scheduler_id(),
            Some(fixture.postcondition.current.clone()),
            state_reads(region_only),
        )
        .expect("region-only source is structurally valid");
        let missing_active_authority =
            VirtualReductionAuthority::new(missing_active, fixture.operation.clone());
        assert!(matches!(
            prepare_virtual(&fixture.persistence, &missing_active_authority),
            Err(VirtualPreparationError::ReadRequired {
                family: VirtualStateFamily::ActiveRegions,
                ..
            })
        ));

        let mut exhausted_operation = fixture.operation.clone();
        let VirtualOperationAuthority::MigrateRegion { command, .. } = &mut exhausted_operation
        else {
            panic!("fixture must carry migration authority");
        };
        command.plan.targets[0].cursor.exhausted = true;
        assert!(matches!(
            try_prepare_from_state(
                &fixture.persistence,
                Some(&fixture.postcondition.current),
                fixture.leaves.clone(),
                &exhausted_operation,
            ),
            Err(ProtocolError::IllegalTransition(_))
        ));

        let reduction = prepare_from_state(
            &fixture.persistence,
            Some(&fixture.postcondition.current),
            fixture.leaves,
            &fixture.operation,
        );
        assert_eq!(reduction.current.counts.regions, 3);
        assert_eq!(reduction.current.counts.active_regions, 2);
        assert_eq!(
            reduction
                .mutations
                .operations
                .iter()
                .filter(|mutation| mutation.family() == VirtualStateFamily::ActiveRegions)
                .count(),
            3
        );

        let mut tampered = reduction.mutations;
        let target = tampered
            .operations
            .iter_mut()
            .find_map(|mutation| match mutation {
                VirtualStateMutation::ActiveRegions {
                    before: None,
                    after: Some(target),
                } => Some(target),
                _ => None,
            })
            .expect("migration inserts active targets");
        target.region_id.push_str(":tampered");
        assert!(matches!(
            tampered.verify(),
            Err(ProtocolError::IdentityMismatch(_))
        ));
    }

    #[test]
    fn mutation_set_accepts_the_exact_migration_width_and_rejects_one_more() {
        let operation = |index| VirtualStateMutation::ActiveRegions {
            before: None,
            after: Some(VirtualActiveRegionCurrent {
                leaf_version: VIRTUAL_ACTIVE_REGION_CURRENT_VERSION.to_owned(),
                scheduler_id: "scheduler:mutation-bound".to_owned(),
                region_id: format!("region:mutation-bound:{index:05}"),
            }),
        };
        let exact = (0..MAX_VIRTUAL_MUTATION_SET_ITEMS)
            .map(operation)
            .collect::<Vec<_>>();
        let sealed = VirtualMutationSet::new(exact.clone())
            .expect("the widest legal migration mutation set seals");
        assert_eq!(sealed.operations.len(), MAX_VIRTUAL_MUTATION_SET_ITEMS);

        let mut oversized = exact;
        oversized.push(operation(MAX_VIRTUAL_MUTATION_SET_ITEMS));
        assert!(matches!(
            VirtualMutationSet::new(oversized),
            Err(ProtocolError::IdentityMismatch(_))
        ));
    }

    #[test]
    fn virtual_state_root_identity_binds_family_node_and_entry_count() {
        let node = cymule_core::content_id("test.virtual-node/1", &())
            .expect("physical node identity derives");
        let empty = virtual_state_root_id(VirtualStateFamily::Work, None, 0)
            .expect("empty descriptor seals");
        let nonempty = virtual_state_root_id(VirtualStateFamily::Work, Some(&node), 1)
            .expect("non-empty descriptor seals");
        assert_ne!(empty, nonempty);
        assert_ne!(
            nonempty,
            virtual_state_root_id(VirtualStateFamily::Runs, Some(&node), 1)
                .expect("another family seals")
        );
        assert!(virtual_state_root_id(VirtualStateFamily::Work, None, 1).is_err());
        assert!(virtual_state_root_id(VirtualStateFamily::Work, Some(&node), 0).is_err());
        assert!(
            virtual_state_root_id(
                VirtualStateFamily::Work,
                Some(&node),
                cymule_core::MAX_EXACT_INTEGER + 1,
            )
            .is_err()
        );
    }

    #[test]
    fn keyed_source_rejects_many_legal_small_leaves_over_aggregate_bound() {
        let pages = (0_u64..70)
            .map(|page| VirtualParkedIndexPage {
                page_version: VIRTUAL_PARKED_INDEX_PAGE_VERSION.to_owned(),
                scheduler_id: "scheduler:aggregate".to_owned(),
                reason: ParkReason::Budget {
                    account: "account:aggregate".to_owned(),
                },
                page,
                work_ids: (0_u64..256)
                    .map(|item| format!("work:{page:03}:{item:03}:{}", "x".repeat(480)))
                    .collect(),
                next_page: (page < 69).then_some(page + 1),
            })
            .collect::<Vec<_>>();
        assert!(pages.iter().all(|page| {
            page.verify().is_ok()
                && cymule_core::canonical_bytes(page)
                    .is_ok_and(|bytes| bytes.len() < MAX_VIRTUAL_KEYED_LEAF_BYTES)
        }));

        let leaves = pages
            .into_iter()
            .map(VirtualStateLeaf::ParkedIndex)
            .collect();
        let error =
            VirtualKeyedSource::from_reads("scheduler:aggregate", None, state_reads(leaves))
                .expect_err("aggregate source bytes must be bounded independently of leaf count");
        assert!(error.to_string().contains("aggregate canonical byte bound"));
    }

    #[test]
    fn keyed_source_counts_proven_absence_at_the_exact_migration_width() {
        let absence = |index| {
            VirtualStateRead::new(
                VirtualStateFamily::Work,
                cymule_core::content_id("test.virtual-negative-read/1", &index)
                    .expect("negative read key derives"),
                None,
            )
            .expect("negative read seals")
        };
        let exact = (0..MAX_VIRTUAL_REDUCTION_SOURCE_ITEMS)
            .map(absence)
            .collect::<Vec<_>>();
        VirtualKeyedSource::from_reads("scheduler:negative-bound", None, exact.clone())
            .expect("the widest legal exact-read source seals");

        let mut oversized = exact;
        oversized.push(absence(MAX_VIRTUAL_REDUCTION_SOURCE_ITEMS));
        let error = VirtualKeyedSource::from_reads("scheduler:negative-bound", None, oversized)
            .expect_err("one additional proven absence exceeds the source item bound");
        assert!(error.to_string().contains("more exact source"));
    }

    #[test]
    fn persistence_receipt_rejects_aggregate_evidence_over_leaf_safe_bound() {
        let payload = cymule_core::artifact_ref("test.virtual-payload/1", b"payload")
            .expect("payload identity derives");
        let source = RegionSourceBinding {
            operation: "test.materialize".to_owned(),
            binding: "source:test".to_owned(),
            revision: "revision:test".to_owned(),
        };
        let expected_cursor = VirtualCursor {
            version: "cursor:test".to_owned(),
            position: "before".to_owned(),
            exhausted: false,
        };
        let command = VirtualPersistenceCommand::new(VirtualPersistenceOperation::Materialize(
            VirtualMaterializationCommand {
                control_version: VIRTUAL_MATERIALIZATION_CONTROL_VERSION.to_owned(),
                scheduler_id: "scheduler:large-receipt".to_owned(),
                command_id: "materialize:large-receipt".to_owned(),
                region_id: "region:large-receipt".to_owned(),
                expected_source: source,
                expected_cursor,
            },
        ))
        .expect("materialization command seals");
        let sibling = cymule_core::content_id("test.virtual-proof-node/1", &())
            .expect("proof sibling derives");
        let mut items = Vec::new();
        let mut proofs = BTreeMap::new();
        for index in 0..600 {
            let work_id = format!("work:large-receipt:{index:04}");
            items.push(WorkItem {
                work_id: work_id.clone(),
                region_id: "region:large-receipt".to_owned(),
                run_id: "run:large-receipt".to_owned(),
                payload: payload.clone(),
                capability: None,
                priority: 0,
                cost: 1,
            });
            proofs.insert(
                work_id.clone(),
                VirtualArchiveWorkProof {
                    proof_version: WORK_INDEX_PROOF_VERSION.to_owned(),
                    work_id,
                    value: None,
                    empty_depth: Some(
                        u16::try_from(WORK_INDEX_DEPTH).expect("index depth fits in u16"),
                    ),
                    siblings: vec![sibling.clone(); WORK_INDEX_DEPTH],
                },
            );
        }
        let evidence = VirtualPersistenceEvidence::Materialized {
            page: MaterializedPage {
                items,
                artifacts: Vec::new(),
                next_cursor: VirtualCursor {
                    version: "cursor:test".to_owned(),
                    position: "after".to_owned(),
                    exhausted: true,
                },
            },
            archived_work_proofs: proofs,
        };
        let mutations = VirtualMutationSet::new(Vec::new()).expect("empty mutation set seals");
        let result_body_id = cymule_core::content_id("test.virtual-current-body/1", &())
            .expect("result body identity derives");
        let error = VirtualPersistenceReceipt::new(
            command,
            Some(
                cymule_core::content_id("test.virtual-current/1", &())
                    .expect("parent identity derives"),
            ),
            evidence,
            mutations,
            result_body_id,
            VirtualPersistenceOutcome::Materialized {
                region_id: "region:large-receipt".to_owned(),
                materialized: 600,
            },
        )
        .expect_err("aggregate receipt must fit one real Durable StateRoot leaf");
        assert!(error.to_string().contains("leaf-safe byte bound"));
    }

    #[test]
    fn resource_lifecycle_reference_rejects_non_archive_virtual_receipts() {
        let (_, postcondition) = initialization_postcondition();
        assert!(
            crate::resource::ResourceLifecycleReceiptRef::from_virtual_compaction(
                &postcondition.receipt,
            )
            .is_err()
        );
        assert!(
            crate::resource::ResourceLifecycleReceiptRef::from_virtual_archive_retirement(
                &postcondition.receipt,
            )
            .is_err()
        );
    }
}
