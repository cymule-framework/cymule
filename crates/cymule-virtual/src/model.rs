use std::collections::{BTreeMap, BTreeSet, VecDeque};

use cymule_core::ArtifactRef;
use serde::{Deserialize, Serialize};

/// Binding-pinned virtual work occurrence version.
pub const VIRTUAL_WORK_OCCURRENCE_VERSION: &str = "cymule.virtual-work-occurrence/1";
/// Provider-neutral virtual work control command version.
pub const VIRTUAL_WORK_CONTROL_VERSION: &str = "cymule.virtual-work-control/1";

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

/// A logical region whose full work set is not materialized eagerly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRegion {
    /// Stable region identity.
    pub region_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Source adapter operation.
    pub source: String,
    /// Current durable cursor.
    pub cursor: VirtualCursor,
    /// Optional logical cardinality estimate for display only.
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
    pub capability: Option<String>,
    /// Relative scheduling priority. Higher values run first within a Run.
    pub priority: i32,
    /// Provider-neutral budget weight.
    pub cost: u64,
}

/// One bounded page returned by a `RegionSource`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedPage {
    /// Newly visible work.
    pub items: Vec<WorkItem>,
    /// Cursor after this exact page.
    pub next_cursor: VirtualCursor,
}

/// Why work is not currently schedulable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
pub struct ClaimedWork {
    /// Work item.
    pub item: WorkItem,
    /// Claim owner.
    pub owner: String,
    /// Monotone per-work fencing epoch.
    pub epoch: u64,
    /// Stable identity of this exact work attempt occurrence.
    pub occurrence_id: String,
    /// Immutable implementation binding selected before claim admission.
    pub occurrence_binding: String,
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
    /// Immutable execution binding selected before the claim was published.
    pub occurrence_binding: String,
    /// Current occurrence lifecycle.
    pub state: WorkOccurrenceState,
    /// Terminal output Artifact for success.
    pub result: Option<ArtifactRef>,
    /// Failure or cancellation evidence Artifact.
    pub error: Option<ArtifactRef>,
    /// Indexed condition used by retry or park, when present.
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
    /// Proposed terminal, retry, park, or cancellation disposition.
    pub resolution: WorkResolution,
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

/// Portable scheduler state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualSnapshot {
    /// Frozen scheduling policy for this checkpoint lineage.
    #[serde(default)]
    pub scheduling_policy: SchedulingPolicy,
    /// Registered virtual regions.
    pub regions: BTreeMap<String, VirtualRegion>,
    /// Ready work grouped by Run.
    pub ready: BTreeMap<String, VecDeque<WorkItem>>,
    /// Active fenced claims.
    pub active: BTreeMap<String, ClaimedWork>,
    /// Parked work indexed by identity.
    pub parked: BTreeMap<String, ParkedWork>,
    /// Derived exact reason-to-work index for bounded wake-up.
    ///
    /// The index is rebuilt from `parked` on restore and is not serialized as
    /// canonical checkpoint data.
    #[serde(skip)]
    pub parked_index: BTreeMap<ParkReason, BTreeSet<String>>,
    /// Every identity already materialized from a source.
    pub known: BTreeSet<String>,
    /// Last Run selected by deterministic round-robin fairness.
    pub last_run: Option<String>,
    /// Last region materialized for deterministic source fairness.
    #[serde(default)]
    pub last_region: Option<String>,
    /// Last claim epoch per work identity.
    pub claim_epochs: BTreeMap<String, u64>,
    /// Binding-pinned attempt occurrences keyed by stable occurrence ID.
    pub occurrences: BTreeMap<String, WorkOccurrence>,
    /// Positive fairness share per Run.
    #[serde(default)]
    pub run_weights: BTreeMap<String, u32>,
    /// Accumulated weighted deficit per Run.
    #[serde(default)]
    pub run_deficits: BTreeMap<String, u64>,
    /// Number of successful scheduling decisions.
    #[serde(default)]
    pub dispatch_sequence: u64,
    /// Dispatch sequence when each currently ready work item became eligible.
    #[serde(default)]
    pub ready_since: BTreeMap<String, u64>,
}
