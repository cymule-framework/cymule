use std::collections::{BTreeMap, BTreeSet, VecDeque};

use cymule_core::ArtifactRef;
use serde::{Deserialize, Serialize};

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

/// Portable scheduler state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualSnapshot {
    /// Registered virtual regions.
    pub regions: BTreeMap<String, VirtualRegion>,
    /// Ready work grouped by Run.
    pub ready: BTreeMap<String, VecDeque<WorkItem>>,
    /// Active fenced claims.
    pub active: BTreeMap<String, ClaimedWork>,
    /// Parked work indexed by identity.
    pub parked: BTreeMap<String, ParkedWork>,
    /// Every identity already materialized from a source.
    pub known: BTreeSet<String>,
    /// Last Run selected by deterministic round-robin fairness.
    pub last_run: Option<String>,
    /// Last claim epoch per work identity.
    pub claim_epochs: BTreeMap<String, u64>,
}
