//! Closed provider-neutral M4 semantic and persistence authority.
//!
//! Durable persistence consumes normalized typed leaves and exact keyed
//! receipts. No current leaf contains replay history, and no wire command can
//! carry a caller-authored provider product or target state.

mod adapters;
mod compatibility;
mod control;
mod controller;
mod linker;
mod live_control;
mod model;
mod persistence;

use std::fmt::{Display, Formatter};

use cymule_core::{ArtifactRecord, Definition};
use serde::{Deserialize, Serialize};

pub use adapters::{
    MIGRATION_SAFE_POINT_VERSION, MigrationAdapter, MigrationAdapterDescriptor,
    MigrationAdapterRequest, MigrationCapabilityChange, MigrationOutput, MigrationPreservation,
    MigrationRequest, MigrationSafePoint, MigrationStateCoverage, RUN_QUIESCENCE_ID_DOMAIN,
    ShadowBindingMode, ShadowDriver, ShadowDriverDescriptor, ShadowEffectMode, ShadowOutput,
    ShadowRequest,
};
pub use compatibility::{
    RELINK_COMPATIBILITY_VERSION, RelinkCompatibility, RelinkViolation, analyze_relink,
};
pub use control::{EVOLUTION_CONTROL_VERSION, EvolutionCommand};
pub use controller::diff_plans;
pub use linker::{
    LinkedPlan, MAX_SUBFLOW_REFERENCE_BYTES, MAX_SUBFLOW_REFERENCE_DEPTH, MAX_SUBFLOW_REFERENCES,
    PlanTemplate, ReferenceStrategy, SUBFLOW_REVISION_VERSION, SubflowReference, SubflowRevision,
};
pub use live_control::{
    LIVE_EVOLUTION_CONTROL_VERSION, LiveEvolutionCommand, LiveEvolutionOutcome,
};
pub use model::{
    GateOutcome, ImpactCone, MigrationReceipt, ObservationOutcome, OccurrencePin, PatchOperation,
    PlanEdge, PlanNode, PlanPatch, RestartReceipt, RestartRequest, RolloutDecision,
    RolloutEvaluation, RolloutGate, RolloutMode, RolloutObservation, RolloutTransition,
    ShadowComparison,
};
pub use persistence::*;

/// Result type for the provider-independent M4 semantic reducer.
pub type EvolutionResult<T> = std::result::Result<T, EvolutionError>;

/// Stable provider-independent M4 failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvolutionError {
    /// A command, model value, or transition is malformed.
    Validation(String),
    /// An executable contract rejected a value.
    Contract(cymule_runtime::ContractViolation),
    /// Exact semantic state required by a command is absent.
    NotFound(String),
    /// One exact `StateRoot` membership or non-membership proof must be loaded
    /// before detached reduction can continue.
    ReadRequired {
        /// Normalized persistent-map family.
        family: EvolutionStateFamily,
        /// Exact content-addressed persistent-map key.
        storage_key: String,
    },
    /// A command conflicts with retained semantic authority.
    Conflict(String),
    /// An exact Scope closure requires bounded paged preparation.
    PagedScopeRequired {
        /// Owning Run.
        run_id: String,
        /// Scope requiring pagination.
        scope_id: String,
        /// Exact source cardinality.
        entries: u64,
    },
    /// A lower collection provider failed without proving corrupt content.
    CollectionProviderFailure(cymule_authenticated_collections::ProviderFailure),
    /// A selected provider returned a malformed closed product.
    PluginDefect {
        /// Stable defect code.
        code: String,
        /// Human-readable defect summary.
        message: String,
    },
    /// A bounded provider invocation was cancelled before an ambiguous dispatch.
    Cancelled {
        /// Stable provider cancellation category.
        code: String,
        /// Human-readable cancellation summary.
        message: String,
    },
    /// A bounded provider invocation timed out before an ambiguous dispatch.
    TimedOut {
        /// Stable provider timeout category.
        code: String,
        /// Human-readable timeout summary.
        message: String,
    },
    /// An identity, causal, encoding, or pinned-source invariant was violated.
    Integrity {
        /// Stable invariant category.
        code: String,
        /// Human-readable invariant summary.
        message: String,
    },
    /// A selected provider substrate failed before producing a semantic value.
    Substrate {
        /// Stable provider substrate category.
        code: String,
        /// Human-readable substrate failure summary.
        message: String,
    },
}

impl Display for EvolutionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
            Self::Contract(error) => write!(formatter, "contract_violation: {error}"),
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::ReadRequired {
                family,
                storage_key,
            } => write!(formatter, "read_required: {family:?} {storage_key}"),
            Self::Conflict(message) => write!(formatter, "conflict: {message}"),
            Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => write!(
                formatter,
                "paged_scope_required: Run {run_id} Scope {scope_id} has {entries} entries"
            ),
            Self::CollectionProviderFailure(failure) => {
                write!(formatter, "collection_provider_failed: {failure}")
            }
            Self::PluginDefect { code, message } => {
                write!(formatter, "plugin_defect ({code}): {message}")
            }
            Self::Cancelled { code, message } => {
                write!(formatter, "cancelled ({code}): {message}")
            }
            Self::TimedOut { code, message } => {
                write!(formatter, "timed_out ({code}): {message}")
            }
            Self::Integrity { code, message } => {
                write!(formatter, "integrity_failed ({code}): {message}")
            }
            Self::Substrate { code, message } => {
                write!(formatter, "substrate_failed ({code}): {message}")
            }
        }
    }
}

impl std::error::Error for EvolutionError {}

impl From<cymule_core::CoreError> for EvolutionError {
    fn from(error: cymule_core::CoreError) -> Self {
        let code = error.code().to_owned();
        match error {
            cymule_core::CoreError::Validation(message) => Self::Validation(message),
            cymule_core::CoreError::NotFound(message) => Self::NotFound(message),
            cymule_core::CoreError::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            },
            cymule_core::CoreError::CollectionProviderFailure(failure) => {
                Self::CollectionProviderFailure(failure)
            }
            cymule_core::CoreError::IllegalTransition(message)
            | cymule_core::CoreError::CommandReuse(message) => Self::Conflict(message),
            cymule_core::CoreError::IdentityMismatch(message)
            | cymule_core::CoreError::Causal(message)
            | cymule_core::CoreError::Encoding(message) => Self::Integrity { code, message },
            error @ (cymule_core::CoreError::PinnedReadSetIncomplete { .. }
            | cymule_core::CoreError::ArchivedCommandReplayRequired { .. }) => Self::Integrity {
                code,
                message: error.to_string(),
            },
        }
    }
}

impl From<cymule_runtime::ContractViolation> for EvolutionError {
    fn from(error: cymule_runtime::ContractViolation) -> Self {
        Self::Contract(error)
    }
}

impl From<cymule_runtime::PlanAdmissionError> for EvolutionError {
    fn from(error: cymule_runtime::PlanAdmissionError) -> Self {
        match error {
            cymule_runtime::PlanAdmissionError::Core(error) => Self::from(error),
            cymule_runtime::PlanAdmissionError::Contract(error) => Self::Contract(error),
        }
    }
}

/// One template affected by a reusable-definition publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveTemplateUpdate {
    /// Stable parent template identity.
    pub template_id: String,
    /// Future Plan before the publication.
    pub previous_plan_id: String,
    /// Future Plan after compatibility admission.
    pub current_plan_id: String,
    /// New rollout decision when the future Plan advanced.
    #[serde(deserialize_with = "model::deserialize_required_nullable")]
    pub decision_id: Option<String>,
    /// Whether the immutable future head advanced.
    pub advanced: bool,
}

/// Result of one atomic reusable-definition publication and relink.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivePublicationReceipt {
    /// Published immutable revision.
    pub revision: SubflowRevision,
    /// Every transitively affected template in stable order.
    pub updates: Vec<LiveTemplateUpdate>,
}

/// Exact publication semantics, including immutable review evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivePublicationCommand {
    /// Reusable definition reference being updated.
    pub logical_ref: String,
    /// Immutable reusable definition content.
    pub definition: Definition,
    /// Strictly logical-reference-ordered exact reusable-definition dependencies.
    pub references: Vec<SubflowReference>,
    /// Review/compiler evidence retained by resulting DAG edges.
    pub evidence: ArtifactRecord,
    /// Future-selection mode for newly linked Plans.
    pub mode: RolloutMode,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use cymule_durable_protocol::{
        CONTINUATION_STATE_VERSION, Continuation, ContinuationStatus, FrameState,
    };

    use super::{EvolutionError, MigrationSafePoint};

    #[test]
    fn core_errors_preserve_semantic_and_integrity_categories() {
        assert!(matches!(
            EvolutionError::from(cymule_core::CoreError::Validation("invalid".to_owned())),
            EvolutionError::Validation(message) if message == "invalid"
        ));
        assert!(matches!(
            EvolutionError::from(cymule_core::CoreError::NotFound("missing".to_owned())),
            EvolutionError::NotFound(message) if message == "missing"
        ));
        assert!(matches!(
            EvolutionError::from(cymule_core::CoreError::CommandReuse("reused".to_owned())),
            EvolutionError::Conflict(message) if message == "reused"
        ));
        assert!(matches!(
            EvolutionError::from(cymule_core::CoreError::IdentityMismatch("forged".to_owned())),
            EvolutionError::Integrity { code, message }
                if code == "identity_mismatch" && message == "forged"
        ));
    }

    #[test]
    fn archived_core_replay_does_not_create_a_physical_evolution_error_variant() {
        let error = EvolutionError::from(cymule_core::CoreError::ArchivedCommandReplayRequired {
            command_id: "command-1".to_owned(),
            archive_head: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                .to_owned(),
            command_index_root:
                "sha256:2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
        });
        assert!(matches!(
            error,
            EvolutionError::Integrity { code, message }
                if code == "archived_command_replay_required"
                    && message.contains("command-1")
        ));
    }

    #[test]
    fn migration_safe_point_pins_a_state_root_content_revision() {
        let domain_revision = cymule_core::content_id("test.state-root/1", &1_u8)
            .expect("test StateRoot revision must derive");
        let plan_id =
            cymule_core::content_id("test.plan/1", &1_u8).expect("test Plan identity must derive");
        let binding_context = cymule_core::content_id("test.binding/1", &1_u8)
            .expect("test binding identity must derive");
        let input = cymule_core::artifact_ref("cymule.test-input/1", b"input").unwrap();
        let continuation = Continuation {
            continuation_version: CONTINUATION_STATE_VERSION.to_owned(),
            run_id: "run-1".to_owned(),
            plan_id,
            binding_context,
            frames: vec![FrameState {
                definition_id: "main".to_owned(),
                invocation_id: "invocation-1".to_owned(),
                invocation_path: Vec::new(),
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                input,
                region_path: Vec::new(),
                next_step: 0,
                locals: BTreeMap::new(),
            }],
            state: None,
            wait_set: BTreeSet::new(),
            scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
            epoch: 1,
            execution_fence: 1,
            execution_claim: None,
            status: ContinuationStatus::Ready,
        };
        let mut safe_point = MigrationSafePoint::new(domain_revision, &continuation)
            .expect("content-addressed StateRoot revision must verify");

        safe_point.domain_revision = cymule_core::sha256_bytes(b"legacy raw revision");
        assert!(safe_point.verify().is_err());
    }
}
