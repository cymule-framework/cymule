//! Agent profile persistence wire authority.
use std::collections::{BTreeMap, BTreeSet};

use crate::resource::{
    ResourceCatalogRecord, ResourceHandle, ResourcePin, ResourcePinCurrent, ResourcePinKind,
    ResourcePinReceipt, ResourceProfilePin, ResourcePublication, ResourceReleaseReceipt,
    ResourceRetentionCurrent, ResourceRetentionSubject, reduce_resource_pin_receipt,
    reduce_resource_reserved_pin_release_receipt,
};
use cymule_core::{
    ArtifactRef, EXECUTION_BINDING_ARTIFACT_KIND, MAX_EXACT_INTEGER, canonical_digest, content_id,
    validate_identity,
};
use cymule_durable_protocol::{
    ClockObservationRef, WAIT_RESULT_ARTIFACT_KIND, WaitOwner, execution_clock_scope,
};
use cymule_runtime::{ContractTarget, ContractValidator};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ProtocolError, ProtocolResult};

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Content-identity domain for one exact Agent-host implementation binding.
pub const AGENT_HOST_BINDING_VERSION: &str = "cymule.agent-host-binding/1";
/// Frozen identity generation for one append-only recovery observation.
pub const AGENT_RECOVERY_OBSERVATION_VERSION: &str = "cymule.agent-recovery-observation/1";
/// Frozen identity generation for one external stream publication intent.
pub const AGENT_STREAM_PUBLICATION_INTENT_VERSION: &str =
    "cymule.agent-stream-publication-intent/2";
/// Frozen persisted pre-publication reservation generation.
pub const AGENT_STREAM_PUBLICATION_RESERVATION_VERSION: &str =
    "cymule.agent-stream-publication-reservation/3";
/// Content identity domain for one exact publication attempt and phase.
pub const AGENT_STREAM_PUBLICATION_DISPATCH_ID_DOMAIN: &str =
    "cymule.agent-stream-publication-dispatch-id/1";
/// Frozen current generation for one exact Agent target claim.
pub const AGENT_TARGET_CLAIM_CURRENT_VERSION: &str = "cymule.agent-target-claim-current/3";
/// Domain-separated key for one Session-local Message or Tool target.
pub const AGENT_TARGET_CLAIM_KEY_DOMAIN: &str = "cymule.agent-target-claim-key/1";
/// Content identity domain for one exact target-claim generation.
pub const AGENT_TARGET_CLAIM_ID_DOMAIN: &str = "cymule.agent-target-claim-id/3";
/// Frozen immutable index-record generation for one exact target-claim generation.
pub const AGENT_TARGET_CLAIM_GENERATION_RECORD_VERSION: &str =
    "cymule.agent-target-claim-generation-record/1";
/// Domain-separated key for one immutable target-claim generation record.
pub const AGENT_TARGET_CLAIM_GENERATION_KEY_DOMAIN: &str =
    "cymule.agent-target-claim-generation-key/1";
const AGENT_OCCURRENCE_TRANSITION_ID_DOMAIN: &str = "cymule.agent-occurrence-transition-id/1";
const AGENT_UPDATE_DIGEST_DOMAIN: &str = "cymule.agent-update-current/1";
const AGENT_MESSAGE_DIGEST_DOMAIN: &str = "cymule.agent-message-current/1";
const AGENT_MESSAGE_HEAD_DOMAIN: &str = "cymule.agent-message-order-head/1";
const AGENT_UNRESOLVED_OCCURRENCE_GENERATION_DOMAIN: &str =
    "cymule.agent-unresolved-occurrence-generation/1";
const AGENT_OPEN_STREAM_GENERATION_DOMAIN: &str = "cymule.agent-open-stream-generation/1";
const AGENT_STREAM_CHUNK_HEAD_DOMAIN: &str = "cymule.agent-stream-chunk-head/1";
const AGENT_TOOL_DERIVED_ID_DOMAIN: &str = "cymule.agent-tool-derived-id/1";

/// Closed semantic purpose of one bounded identity derived for an Agent tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolDerivedPurpose {
    /// The permission request governing one exact tool call.
    PermissionRequest,
    /// The tool's one result message, whether execution was allowed or denied.
    ToolMessage,
}

/// Derive one bounded tool identity without extending a caller's identity.
///
/// The complete Session, tool call, and closed purpose share one canonical
/// preimage, so maximal Unicode identities remain valid and cannot collide
/// across Sessions or semantic uses.
///
/// # Errors
///
/// Returns an error when either semantic identity is invalid or canonical
/// content identity derivation fails.
pub fn agent_tool_derived_id(
    session_id: &str,
    tool_call_id: &str,
    purpose: AgentToolDerivedPurpose,
) -> ProtocolResult<String> {
    validate_identity("Agent Session", session_id)?;
    validate_identity("Agent tool call", tool_call_id)?;
    content_id(
        AGENT_TOOL_DERIVED_ID_DOMAIN,
        &(session_id, tool_call_id, purpose),
    )
    .map_err(Into::into)
}

/// Maximum entries returned by any ordinary Agent profile page query.
pub const MAX_AGENT_PAGE: usize = 256;
/// Maximum canonical bytes accepted for one ordinary Agent value.
pub const MAX_AGENT_VALUE_BYTES: usize = 256 * 1024;
/// Maximum canonical bytes for one keyed current wrapper around a bounded value.
pub const MAX_AGENT_CURRENT_BYTES: usize = 512 * 1024;
/// Maximum canonical bytes returned by one ordinary Agent page query.
pub const MAX_AGENT_PAGE_BYTES: usize = 4 * 1024 * 1024;
/// Maximum entries accepted in one bounded Agent Plan or content vector.
pub const MAX_AGENT_VALUE_ENTRIES: usize = 256;
/// Maximum distinct reconciliation observations retained by one occurrence.
pub const MAX_AGENT_RECOVERY_OBSERVATIONS: usize = 64;
/// Maximum concurrently non-terminal tools retained by one Session.
pub const MAX_AGENT_NONTERMINAL_TOOLS: usize = 64;
/// Maximum summed before/Cancelled canonical bytes reserved for Session close.
pub const MAX_AGENT_TOOL_CLOSE_BYTES: usize = 4 * 1024 * 1024;
/// Maximum canonical bytes of one complete Agent command receipt leaf.
pub const MAX_AGENT_RECEIPT_BYTES: usize = 10 * 1024 * 1024;
/// Maximum canonical bytes of one complete closed Agent command.
pub const MAX_AGENT_COMMAND_BYTES: usize = 2 * 1024 * 1024;
/// Maximum ordered message entries one context capability may inspect.
pub const MAX_AGENT_CONTEXT_SCAN_ENTRIES: u64 = 4_096;
/// Maximum canonical message bytes one context capability may inspect.
pub const MAX_AGENT_CONTEXT_SCAN_BYTES: u64 = 16 * 1024 * 1024;
/// Current bounded Session metadata wire generation.
pub const AGENT_SESSION_CURRENT_VERSION: &str = "cymule.agent-session-current/2";

/// Typed content shared by messages, model output, tools, and artifacts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentBlock {
    /// UTF-8 text.
    Text {
        /// UTF-8 content.
        text: String,
    },
    /// Structured canonical JSON.
    Json {
        /// Structured JSON content.
        value: Value,
    },
    /// Immutable Cymule artifact.
    Artifact {
        /// Immutable Artifact reference.
        artifact: ArtifactRef,
    },
    /// Provider-neutral cross-Run Resource Handle.
    ResourceHandle {
        /// Verified provider-neutral Resource Handle.
        resource: Box<ResourceHandle>,
    },
}

impl ContentBlock {
    /// Verify one finalized content value and every nested reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the block is malformed, exceeds its size bound, or
    /// contains an invalid nested artifact or Resource Handle.
    pub fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Artifact { artifact } => artifact
                .validate()
                .map_err(|error| ProtocolError::Validation(error.to_string()))?,
            Self::ResourceHandle { resource } => resource
                .verify()
                .map_err(|error| ProtocolError::Validation(error.to_string()))?,
            Self::Json { value } => {
                canonical_digest(value)
                    .map_err(|error| ProtocolError::Validation(error.to_string()))?;
            }
            Self::Text { .. } => {}
        }
        validate_canonical_size("Agent content block", self, MAX_AGENT_VALUE_BYTES)
    }
}

fn validate_content_blocks(content: &[ContentBlock]) -> ProtocolResult<()> {
    validate_count(
        "Agent content blocks",
        content.len(),
        MAX_AGENT_VALUE_ENTRIES,
    )?;
    for block in content {
        block.validate()?;
    }
    validate_canonical_size("Agent content", content, MAX_AGENT_VALUE_BYTES)
}

fn validate_count(kind: &str, actual: usize, limit: usize) -> ProtocolResult<()> {
    if actual > limit {
        return Err(ProtocolError::Validation(format!(
            "{kind} exceeds {limit} entries"
        )));
    }
    Ok(())
}

fn validate_canonical_size<T: Serialize + ?Sized>(
    kind: &str,
    value: &T,
    limit: usize,
) -> ProtocolResult<()> {
    let actual = cymule_core::canonical_bytes(&value)?.len();
    if actual > limit {
        return Err(ProtocolError::Validation(format!(
            "{kind} occupies {actual} canonical bytes; maximum is {limit}"
        )));
    }
    Ok(())
}

fn validate_content_token(kind: &str, value: &str, limit: usize) -> ProtocolResult<()> {
    if value.is_empty() || value.chars().count() > limit || value.chars().any(char::is_control) {
        return Err(ProtocolError::Validation(format!(
            "Agent {kind} must contain 1..={limit} non-control characters"
        )));
    }
    Ok(())
}

fn validate_message(message: &AgentMessage) -> ProtocolResult<()> {
    validate_content_token("message identity", &message.message_id, 512)?;
    validate_content_blocks(&message.content)
}

fn validate_message_source_descriptor(
    kind: &str,
    head: Option<&str>,
    count: u64,
) -> ProtocolResult<()> {
    if count > MAX_EXACT_INTEGER || (count == 0) != head.is_none() {
        return Err(ProtocolError::Validation(format!(
            "Agent {kind} message count does not match its exact source head"
        )));
    }
    if let Some(head) = head {
        validate_sha256(&format!("Agent {kind} message head"), head)?;
    }
    Ok(())
}

fn validate_tool_request(request: &ToolRequest) -> ProtocolResult<()> {
    validate_content_token("tool identity", &request.tool_call_id, 512)?;
    validate_content_token("tool operation", &request.operation, 512)?;
    validate_canonical_size("Agent tool request", request, MAX_AGENT_VALUE_BYTES)
}

fn validate_context_snapshot(snapshot: &ContextSnapshot) -> ProtocolResult<()> {
    validate_content_token("context snapshot identity", &snapshot.snapshot_id, 512)?;
    validate_content_token(
        "context occurrence binding",
        &snapshot.occurrence_binding,
        512,
    )?;
    validate_message_source_descriptor(
        "context source",
        snapshot.source_message_head.as_deref(),
        snapshot.source_message_count,
    )?;
    validate_count(
        "Agent context selected messages",
        snapshot.selected_messages.len(),
        MAX_AGENT_VALUE_ENTRIES,
    )?;
    let mut previous = None;
    for selected in &snapshot.selected_messages {
        validate_content_token("Agent context message", &selected.message_id, 512)?;
        validate_sha256("Agent context message digest", &selected.message_digest)?;
        if selected.index > MAX_EXACT_INTEGER
            || selected.index >= snapshot.source_message_count
            || previous.is_some_and(|index| selected.index <= index)
        {
            return Err(ProtocolError::Validation(
                "Agent context message bindings must be strictly ordered exact integers".to_owned(),
            ));
        }
        previous = Some(selected.index);
    }
    validate_content_blocks(&snapshot.content)?;
    validate_canonical_size("Agent context snapshot", snapshot, MAX_AGENT_VALUE_BYTES)
}

fn compile_elicitation_schema(request: &ElicitationRequest) -> ProtocolResult<ContractValidator> {
    ContractValidator::compile(
        ContractTarget::wait(request.request_id.clone()),
        &request.schema,
    )
    .map_err(ProtocolError::from)
}

fn validate_elicitation_request(request: &ElicitationRequest) -> ProtocolResult<()> {
    validate_content_token("elicitation request identity", &request.request_id, 512)?;
    validate_content_blocks(&request.prompt)?;
    compile_elicitation_schema(request)?;
    validate_canonical_size("Agent elicitation request", request, MAX_AGENT_VALUE_BYTES)
}

fn validate_elicitation_response(
    request: &ElicitationRequest,
    response: &ElicitationResponse,
) -> ProtocolResult<()> {
    response.validate()?;
    if response.request_id != request.request_id {
        return Err(ProtocolError::Validation(
            "elicitation response identity does not match its request".to_owned(),
        ));
    }
    if let Some(value) = &response.value {
        compile_elicitation_schema(request)?.validate(value)?;
    }
    validate_canonical_size(
        "Agent elicitation response",
        response,
        MAX_AGENT_VALUE_BYTES,
    )
}

/// Message author role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// Caller or human input.
    User,
    /// Agent-produced output.
    Agent,
    /// Tool-produced output.
    Tool,
    /// System or policy context.
    System,
}

/// Durable message projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMessage {
    /// Stable message identity.
    pub message_id: String,
    /// Message author role.
    pub role: MessageRole,
    /// Finalized content blocks.
    pub content: Vec<ContentBlock>,
}

/// Session activity state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    /// No foreground interaction is active.
    Idle,
    /// A caller-owned interaction is active.
    Running,
    /// Durable external input is required.
    RequiresAction,
    /// The Session cannot accept later updates.
    Closed,
}

/// Terminal foreground stop reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStopReason {
    /// The caller-owned turn reached its ordinary boundary.
    EndTurn,
    /// The caller cancelled the foreground work.
    Cancelled,
    /// The agent or policy refused the requested work.
    Refusal,
    /// The caller ended the foreground work after an error.
    Error,
}

/// Plan entry lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanEntryStatus {
    /// Work has not started.
    Pending,
    /// Work is currently active.
    InProgress,
    /// Work completed.
    Completed,
    /// Work was cancelled.
    Cancelled,
}

/// One user-visible Plan entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPlanEntry {
    /// Stable entry identity.
    pub entry_id: String,
    /// User-visible description.
    pub content: String,
    /// Current projected lifecycle.
    pub status: PlanEntryStatus,
}

/// User-visible agent Plan projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPlan {
    /// Stable user-visible Plan identity.
    pub plan_id: String,
    /// Ordered Plan entries.
    pub entries: Vec<AgentPlanEntry>,
}

/// Tool call lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    /// Tool request is known but not admitted.
    Pending,
    /// Tool request is waiting for a separate permission decision.
    AwaitingPermission,
    /// Tool execution may have started.
    InProgress,
    /// Tool execution completed with output.
    Completed,
    /// Tool execution terminated with failure evidence.
    Failed,
    /// Tool execution was cancelled.
    Cancelled,
}

/// Tool call projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    /// Stable caller-owned tool occurrence identity.
    pub tool_call_id: String,
    /// Abstract tool operation.
    pub operation: String,
    /// Current projected lifecycle.
    pub status: ToolCallStatus,
    /// Immutable structured input.
    pub input: Value,
    /// Finalized output, when available.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub output: Option<Vec<ContentBlock>>,
    /// Optional presentation locations attached by an adapter.
    pub locations: Vec<String>,
}

/// Token and monetary usage projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Usage {
    /// Cumulative consumed units.
    pub used: u64,
    /// Reported capacity or budget ceiling.
    pub capacity: u64,
    /// Optional provider-neutral structured cost observation.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub cost: Option<Value>,
}

/// Idempotent ordered update applied to one Session projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentUpdate {
    /// Publish one finalized message.
    Message {
        /// Stable update identity.
        update_id: String,
        /// Finalized message.
        message: AgentMessage,
    },
    /// Change the Session activity projection.
    State {
        /// Stable update identity.
        update_id: String,
        /// New Session activity state.
        state: AgentState,
        /// Required reason when returning to idle.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        stop_reason: Option<SessionStopReason>,
    },
    /// Replace the current user-visible agent Plan.
    Plan {
        /// Stable update identity.
        update_id: String,
        /// Complete current Plan projection.
        plan: AgentPlan,
    },
    /// Advance one tool lifecycle projection.
    Tool {
        /// Stable update identity.
        update_id: String,
        /// Complete current tool projection.
        tool: ToolCall,
    },
    /// Replace the cumulative usage observation.
    Usage {
        /// Stable update identity.
        update_id: String,
        /// Latest cumulative usage.
        usage: Usage,
    },
    /// Create or resolve one durable elicitation.
    Elicitation {
        /// Stable update identity.
        update_id: String,
        /// Complete current elicitation projection.
        elicitation: ElicitationProjection,
    },
}

impl AgentUpdate {
    /// Stable idempotency identity for this update.
    pub fn update_id(&self) -> &str {
        match self {
            Self::Message { update_id, .. }
            | Self::State { update_id, .. }
            | Self::Plan { update_id, .. }
            | Self::Tool { update_id, .. }
            | Self::Usage { update_id, .. }
            | Self::Elicitation { update_id, .. } => update_id,
        }
    }

    /// Verify the complete update without reading current durable state.
    ///
    /// # Errors
    ///
    /// Returns an error when the update identity, payload, or variant-specific
    /// invariant is invalid or exceeds its bounded wire size.
    pub fn validate_content(&self) -> ProtocolResult<()> {
        validate_content_token("update identity", self.update_id(), 512)?;
        match self {
            Self::Message { message, .. } => validate_message(message),
            Self::State {
                state, stop_reason, ..
            } => {
                if (*state == AgentState::Idle) != stop_reason.is_some() {
                    return Err(ProtocolError::Validation(
                        "only an idle transition carries exactly one stop_reason".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::Plan { plan, .. } => validate_agent_plan(plan),
            Self::Tool { tool, .. } => validate_tool_projection(tool),
            Self::Usage { usage, .. } => validate_usage(usage),
            Self::Elicitation { elicitation, .. } => elicitation.validate(),
        }?;
        validate_canonical_size("Agent update", self, MAX_AGENT_VALUE_BYTES)
    }
}

/// Kind of bounded transition that most recently changed Session metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSessionTransitionKind {
    /// One direct Session update.
    SessionUpdate,
    /// One host-occurrence lifecycle transition.
    Occurrence,
    /// One stream open, abort, or finalization transition.
    Stream,
    /// One input suspension or completion.
    Input,
    /// One M1 workspace scope/effect transition.
    Workspace,
}

/// Typed reference to the exact command that produced current Session metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionTransitionWitness {
    /// Exact closed Agent command identity.
    pub command_id: String,
    /// Bounded Session-affecting transition kind.
    pub kind: AgentSessionTransitionKind,
}

impl AgentSessionTransitionWitness {
    /// Verify the self-contained typed command reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the referenced command identity is not canonical.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_sha256("Agent Session transition command", &self.command_id)
    }
}

/// Bounded exact membership and future-close charge for one non-terminal Tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNonterminalTool {
    /// Canonical digest of the independently keyed current Tool projection.
    pub current_digest: String,
    /// Summed canonical bytes of that current and its Cancelled successor.
    pub close_bytes: u64,
}

impl AgentNonterminalTool {
    fn new(current: &AgentToolCurrent) -> ProtocolResult<Self> {
        let entry = Self {
            current_digest: canonical_digest(current)?,
            close_bytes: current.nonterminal_close_charge()?,
        };
        entry.verify_for(current)?;
        Ok(entry)
    }

    /// Verify the bounded self-contained directory entry.
    ///
    /// # Errors
    ///
    /// Returns an error when the digest or byte charge is malformed.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_canonical_digest(
            "Agent non-terminal Tool current digest",
            &self.current_digest,
        )?;
        if self.close_bytes == 0 || self.close_bytes > MAX_EXACT_INTEGER {
            return Err(ProtocolError::Validation(
                "Agent non-terminal Tool close charge is outside the exact integer range"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Verify exact coupling to one independently keyed non-terminal Tool current.
    ///
    /// # Errors
    ///
    /// Returns an error when the current is terminal, invalid, or differs in
    /// canonical content or close charge.
    pub fn verify_for(&self, current: &AgentToolCurrent) -> ProtocolResult<()> {
        self.verify()?;
        if current.tool.status.is_terminal()
            || self.current_digest != canonical_digest(current)?
            || self.close_bytes != current.nonterminal_close_charge()?
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent non-terminal Tool directory entry differs from its exact current".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Bounded keyed current metadata for one Agent Session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionCurrent {
    /// Frozen current-projection generation.
    pub current_version: String,
    /// Stable Session identity.
    pub session_id: String,
    /// Current activity state.
    pub state: AgentState,
    /// Required reason exactly while idle after a foreground transition.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub stop_reason: Option<SessionStopReason>,
    /// Current bounded Plan projection.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub plan: Option<AgentPlan>,
    /// Latest bounded usage projection.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub usage: Option<Usage>,
    /// Largest numeric Session update suffix observed so far.
    pub latest_update_sequence: u64,
    /// Next ordinal allocated to a newly prepared host occurrence.
    pub next_occurrence_sequence: u64,
    /// Number of immutable message entries in the ordered Session log.
    pub message_count: u64,
    /// Content head of the append-only message order.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub message_head: Option<String>,
    /// Number of elicitations which still have no response.
    pub pending_elicitation_count: u64,
    /// Bounded capacity directory for every non-terminal Tool current.
    ///
    /// Each key is an exact Tool identity and each value binds the current's
    /// canonical digest plus the summed byte charge of that current and its
    /// deterministic Cancelled successor. The independently keyed Tool current
    /// remains the sole lifecycle authority; this directory proves bounded
    /// close completeness.
    pub nonterminal_tools: BTreeMap<String, AgentNonterminalTool>,
    /// Number of Prepared, Started, or Unknown host occurrences.
    pub unresolved_occurrence_count: u64,
    /// Generation of the deletable unresolved-occurrence index.
    pub unresolved_occurrence_generation: String,
    /// Number of streams which have not finalized or aborted.
    pub open_stream_count: u64,
    /// Generation of the deletable open-stream index.
    pub open_stream_generation: String,
    /// Exact bounded transition which produced this metadata, absent at genesis.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub last_transition: Option<AgentSessionTransitionWitness>,
}

impl AgentSessionCurrent {
    /// Construct empty bounded Session metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the Session identity is invalid or the canonical
    /// genesis projections cannot be derived.
    pub fn new(session_id: impl Into<String>) -> ProtocolResult<Self> {
        let session_id = session_id.into();
        validate_identity("Agent Session", &session_id)?;
        let unresolved_occurrence_generation = content_id(
            AGENT_UNRESOLVED_OCCURRENCE_GENERATION_DOMAIN,
            &(session_id.as_str(), 0_u64),
        )?;
        let open_stream_generation = content_id(
            AGENT_OPEN_STREAM_GENERATION_DOMAIN,
            &(session_id.as_str(), 0_u64),
        )?;
        let current = Self {
            current_version: AGENT_SESSION_CURRENT_VERSION.to_owned(),
            session_id,
            state: AgentState::Idle,
            stop_reason: None,
            plan: None,
            usage: None,
            latest_update_sequence: 0,
            next_occurrence_sequence: 0,
            message_count: 0,
            message_head: None,
            pending_elicitation_count: 0,
            nonterminal_tools: BTreeMap::new(),
            unresolved_occurrence_count: 0,
            unresolved_occurrence_generation,
            open_stream_count: 0,
            open_stream_generation,
            last_transition: None,
        };
        current.verify()?;
        Ok(current)
    }

    /// Verify the complete bounded current projection.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata, counters, state coupling, identities, or
    /// bounded nested values violate the Session-current contract.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_agent_version(
            "Agent Session current",
            &self.current_version,
            AGENT_SESSION_CURRENT_VERSION,
        )?;
        validate_identity("Agent Session", &self.session_id)?;
        for (kind, value) in [
            ("latest update sequence", self.latest_update_sequence),
            ("next occurrence sequence", self.next_occurrence_sequence),
            ("message count", self.message_count),
            ("pending elicitation count", self.pending_elicitation_count),
            (
                "unresolved occurrence count",
                self.unresolved_occurrence_count,
            ),
            ("open stream count", self.open_stream_count),
        ] {
            if value > MAX_EXACT_INTEGER {
                return Err(ProtocolError::Validation(format!(
                    "Agent Session {kind} exceeds the exact integer range"
                )));
            }
        }
        if (self.message_count == 0) != self.message_head.is_none() {
            return Err(ProtocolError::Validation(
                "Agent Session message count does not match its order head".to_owned(),
            ));
        }
        if let Some(head) = &self.message_head {
            validate_sha256("Agent Session message head", head)?;
        }
        validate_sha256(
            "Agent unresolved occurrence generation",
            &self.unresolved_occurrence_generation,
        )?;
        validate_sha256("Agent open stream generation", &self.open_stream_generation)?;
        if (self.state == AgentState::RequiresAction) != (self.pending_elicitation_count > 0) {
            return Err(ProtocolError::Validation(
                "Agent Session RequiresAction state does not match pending elicitations".to_owned(),
            ));
        }
        if self.nonterminal_tools.len() > MAX_AGENT_NONTERMINAL_TOOLS {
            return Err(ProtocolError::Validation(format!(
                "Agent Session exceeds {MAX_AGENT_NONTERMINAL_TOOLS} non-terminal tools"
            )));
        }
        let mut close_bytes = 0_u64;
        for (tool_call_id, entry) in &self.nonterminal_tools {
            validate_content_token("non-terminal Agent tool identity", tool_call_id, 512)?;
            entry.verify()?;
            close_bytes = close_bytes.checked_add(entry.close_bytes).ok_or_else(|| {
                ProtocolError::Validation(
                    "Agent non-terminal tool close charge is exhausted".to_owned(),
                )
            })?;
        }
        if close_bytes > MAX_AGENT_TOOL_CLOSE_BYTES as u64 {
            return Err(ProtocolError::Validation(format!(
                "Agent non-terminal tools exceed {MAX_AGENT_TOOL_CLOSE_BYTES} close bytes"
            )));
        }
        if self.state != AgentState::Idle && self.stop_reason.is_some() {
            return Err(ProtocolError::Validation(
                "only an idle Agent Session may retain a stop reason".to_owned(),
            ));
        }
        if self.state == AgentState::Closed
            && (self.unresolved_occurrence_count != 0
                || self.open_stream_count != 0
                || !self.nonterminal_tools.is_empty())
        {
            return Err(ProtocolError::Validation(
                "closed Agent Session cannot retain unresolved occurrences, open streams, or non-terminal tools".to_owned(),
            ));
        }
        if let Some(plan) = &self.plan {
            validate_agent_plan(plan)?;
        }
        if let Some(usage) = &self.usage {
            validate_usage(usage)?;
        }
        if let Some(last_transition) = &self.last_transition {
            last_transition.verify()?;
        }
        validate_canonical_size("Agent Session current", self, MAX_AGENT_VALUE_BYTES)
    }
}

/// Payload-free append authority for one immutable ordered Session message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMessageOrderEntry {
    /// Owning Session identity.
    pub session_id: String,
    /// Zero-based immutable message ordinal.
    pub index: u64,
    /// Stable message identity.
    pub message_id: String,
    /// Content identity of the complete message payload.
    pub message_digest: String,
    /// Previous order head, absent only for index zero.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub previous_head: Option<String>,
    /// Content identity of this complete order entry.
    pub head: String,
    /// Exact Agent command that admitted this entry.
    pub admitted_by: String,
}

impl AgentMessageOrderEntry {
    fn new(
        session: &AgentSessionCurrent,
        command_id: &str,
        message: &AgentMessage,
    ) -> ProtocolResult<Self> {
        let message_digest = content_id(AGENT_MESSAGE_DIGEST_DOMAIN, message)?;
        let head = content_id(
            AGENT_MESSAGE_HEAD_DOMAIN,
            &(
                session.session_id.as_str(),
                session.message_count,
                message.message_id.as_str(),
                message_digest.as_str(),
                session.message_head.as_deref(),
                command_id,
            ),
        )?;
        let entry = Self {
            session_id: session.session_id.clone(),
            index: session.message_count,
            message_id: message.message_id.clone(),
            message_digest,
            previous_head: session.message_head.clone(),
            head,
            admitted_by: command_id.to_owned(),
        };
        entry.verify()?;
        Ok(entry)
    }

    /// Verify content identity, ordinal predecessor shape, and command reference.
    ///
    /// # Errors
    ///
    /// Returns an error when any identity, ordinal, predecessor, digest, or size
    /// does not match this immutable order entry.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        validate_identity("Agent message", &self.message_id)?;
        if self.index > MAX_EXACT_INTEGER {
            return Err(ProtocolError::Validation(
                "Agent message index exceeds the exact integer range".to_owned(),
            ));
        }
        if (self.index == 0) != self.previous_head.is_none() {
            return Err(ProtocolError::Validation(
                "Agent message order predecessor does not match its index".to_owned(),
            ));
        }
        validate_sha256("Agent message payload", &self.message_digest)?;
        if let Some(previous_head) = &self.previous_head {
            validate_sha256("Agent previous message head", previous_head)?;
        }
        validate_sha256("Agent message command", &self.admitted_by)?;
        let expected = content_id(
            AGENT_MESSAGE_HEAD_DOMAIN,
            &(
                self.session_id.as_str(),
                self.index,
                self.message_id.as_str(),
                self.message_digest.as_str(),
                self.previous_head.as_deref(),
                self.admitted_by.as_str(),
            ),
        )?;
        if self.head != expected {
            return Err(ProtocolError::IdentityMismatch(
                "Agent message order head does not match its content".to_owned(),
            ));
        }
        validate_canonical_size("Agent message order entry", self, MAX_AGENT_VALUE_BYTES)
    }
}

/// Keyed immutable message payload authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMessageCurrent {
    /// Owning Session identity.
    pub session_id: String,
    /// Complete immutable message payload.
    pub message: AgentMessage,
    /// Matching payload-free order entry.
    pub order: AgentMessageOrderEntry,
}

impl AgentMessageCurrent {
    /// Verify exact Session, message, digest, and order coupling.
    ///
    /// # Errors
    ///
    /// Returns an error when the message is invalid or does not exactly match
    /// its owning Session and immutable order entry.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_message(&self.message)?;
        self.order.verify()?;
        if self.session_id != self.order.session_id
            || self.message.message_id != self.order.message_id
            || content_id(AGENT_MESSAGE_DIGEST_DOMAIN, &self.message)? != self.order.message_digest
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent message current does not match its order authority".to_owned(),
            ));
        }
        validate_canonical_size("Agent message current", self, MAX_AGENT_CURRENT_BYTES)
    }
}

fn message_page_entry_canonical_bytes(entries: &[AgentMessageCurrent]) -> ProtocolResult<usize> {
    entries.iter().try_fold(0_usize, |total, entry| {
        let bytes = cymule_core::canonical_bytes(entry)?.len();
        total.checked_add(bytes).ok_or_else(|| {
            ProtocolError::Validation("Agent message page entry byte count is exhausted".to_owned())
        })
    })
}

/// One bounded head-pinned page of ordered Session messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMessagePage {
    /// Owning Session identity.
    pub session_id: String,
    /// Exact message-order head pinned by this scan.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub expected_message_head: Option<String>,
    /// Exact number of messages in the immutable source prefix.
    pub source_message_count: u64,
    /// Exclusive ordinal at which this backward page ends; absent means source-prefix count.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub end_exclusive: Option<u64>,
    /// Entries returned in ascending ordinal order.
    pub entries: Vec<AgentMessageCurrent>,
    /// Earlier exclusive end for the next page, absent at the beginning.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub next_end_exclusive: Option<u64>,
}

impl AgentMessagePage {
    /// Verify page bounds, immutable owner, contiguous order, and cursor shape.
    ///
    /// # Errors
    ///
    /// Returns an error when the page exceeds its budgets or its owner, order,
    /// pinned head, entries, or cursors are inconsistent.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        validate_message_source_descriptor(
            "message page source",
            self.expected_message_head.as_deref(),
            self.source_message_count,
        )?;
        if self
            .end_exclusive
            .is_some_and(|value| value > MAX_EXACT_INTEGER || value > self.source_message_count)
            || self
                .next_end_exclusive
                .is_some_and(|value| value > MAX_EXACT_INTEGER || value > self.source_message_count)
        {
            return Err(ProtocolError::Validation(
                "Agent message page cursor exceeds the exact integer range".to_owned(),
            ));
        }
        if self.entries.len() > MAX_AGENT_PAGE {
            return Err(ProtocolError::Validation(format!(
                "Agent message page exceeds {MAX_AGENT_PAGE} entries"
            )));
        }
        let effective_end = self.end_exclusive.unwrap_or(self.source_message_count);
        let mut previous_index = None;
        let mut previous_head: Option<&str> = None;
        for entry in &self.entries {
            entry.verify()?;
            if entry.session_id != self.session_id
                || previous_index.is_some_and(|index| entry.order.index != index + 1)
                || previous_head
                    .is_some_and(|head| entry.order.previous_head.as_deref() != Some(head))
            {
                return Err(ProtocolError::Validation(
                    "Agent message page is not one contiguous ascending range".to_owned(),
                ));
            }
            previous_index = Some(entry.order.index);
            previous_head = Some(&entry.order.head);
        }
        if let Some(last) = previous_index
            && last.checked_add(1) != Some(effective_end)
        {
            return Err(ProtocolError::Validation(
                "Agent message page does not end at its exclusive cursor".to_owned(),
            ));
        }
        if effective_end == self.source_message_count
            && previous_head != self.expected_message_head.as_deref()
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent message page terminal entry does not match its pinned head".to_owned(),
            ));
        }
        let expected_next = self
            .entries
            .first()
            .and_then(|entry| (entry.order.index > 0).then_some(entry.order.index));
        if self.next_end_exclusive != expected_next {
            return Err(ProtocolError::Validation(
                "Agent message page next cursor does not match its first entry".to_owned(),
            ));
        }
        if self.entries.is_empty() && self.next_end_exclusive.is_some() {
            return Err(ProtocolError::Validation(
                "empty Agent message page cannot carry a next cursor".to_owned(),
            ));
        }
        if self.entries.is_empty() && effective_end > 0 {
            return Err(ProtocolError::Validation(
                "Agent message page with remaining history must advance its cursor".to_owned(),
            ));
        }
        if message_page_entry_canonical_bytes(&self.entries)? > MAX_AGENT_PAGE_BYTES {
            return Err(ProtocolError::Validation(format!(
                "Agent message page entries exceed {MAX_AGENT_PAGE_BYTES} canonical bytes"
            )));
        }
        validate_canonical_size("Agent message page", self, MAX_AGENT_PAGE_BYTES)
    }
}

/// Exact Session metadata lookup pinned to an optional `StateRoot` revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionQuery {
    /// Stable Session identity.
    pub session_id: String,
    /// Exact revision constraint, absent to read the current head once.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub expected_revision: Option<String>,
}

impl AgentSessionQuery {
    /// Verify owner and optional revision constraint.
    ///
    /// # Errors
    ///
    /// Returns an error when the Session identity or revision constraint is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        verify_optional_revision(self.expected_revision.as_ref())
    }
}

/// Backward ordered-message page query pinned to one immutable Session head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMessagePageQuery {
    /// Stable Session identity.
    pub session_id: String,
    /// Exact message head from Session metadata, required-nullable for an empty Session.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub expected_message_head: Option<String>,
    /// Exact number of messages in the immutable source prefix.
    pub source_message_count: u64,
    /// Exclusive backward cursor within the source prefix, absent for its first page.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub end_exclusive: Option<u64>,
    /// Maximum entries returned, within `1..=MAX_AGENT_PAGE`.
    pub max_entries: u64,
    /// Maximum summed canonical bytes of returned message currents.
    pub max_message_canonical_bytes: u64,
    /// Maximum canonical bytes of the complete page-read wire.
    pub max_canonical_bytes: u64,
    /// Exact revision constraint, absent to pin the current head once.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub expected_revision: Option<String>,
}

impl AgentMessagePageQuery {
    /// Verify the pinned head, cursor, and hard page budgets.
    ///
    /// # Errors
    ///
    /// Returns an error when an identity, head, cursor, revision, or requested
    /// page budget is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        validate_message_source_descriptor(
            "message query source",
            self.expected_message_head.as_deref(),
            self.source_message_count,
        )?;
        if self
            .end_exclusive
            .is_some_and(|value| value > MAX_EXACT_INTEGER || value > self.source_message_count)
            || self.max_entries == 0
            || self.max_entries > MAX_AGENT_PAGE as u64
            || self.max_message_canonical_bytes == 0
            || self.max_message_canonical_bytes > MAX_AGENT_PAGE_BYTES as u64
            || self.max_canonical_bytes == 0
            || self.max_canonical_bytes > MAX_AGENT_PAGE_BYTES as u64
        {
            return Err(ProtocolError::Validation(
                "Agent message page query exceeds its cursor or page budget".to_owned(),
            ));
        }
        verify_optional_revision(self.expected_revision.as_ref())
    }
}

macro_rules! agent_exact_query {
    ($name:ident, $field:ident, $kind:literal) => {
        #[doc = concat!("Exact keyed ", $kind, " lookup.")]
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            /// Stable Session identity.
            pub session_id: String,
            #[doc = concat!("Stable ", $kind, " identity.")]
            pub $field: String,
            /// Exact revision constraint, absent to read the current head once.
            #[serde(deserialize_with = "deserialize_required_nullable")]
            pub expected_revision: Option<String>,
        }

        impl $name {
            /// Verify owner, exact key, and optional revision constraint.
            ///
            /// # Errors
            ///
            /// Returns an error when the owner, exact key, or revision constraint is invalid.
            pub fn verify(&self) -> ProtocolResult<()> {
                validate_identity("Agent Session", &self.session_id)?;
                validate_identity(concat!("Agent ", $kind), &self.$field)?;
                verify_optional_revision(self.expected_revision.as_ref())
            }
        }
    };
}

agent_exact_query!(AgentMessageQuery, message_id, "message");
agent_exact_query!(AgentToolQuery, tool_call_id, "tool");
agent_exact_query!(AgentElicitationQuery, request_id, "elicitation");
agent_exact_query!(AgentOccurrenceQuery, occurrence_id, "occurrence");
agent_exact_query!(AgentStreamQuery, stream_id, "stream");

fn verify_optional_revision(revision: Option<&String>) -> ProtocolResult<()> {
    if let Some(revision) = revision {
        validate_sha256("Agent query revision", revision)?;
    }
    Ok(())
}

fn verify_read_revision(actual: &str, expected: Option<&String>) -> ProtocolResult<()> {
    validate_sha256("Agent read revision", actual)?;
    if expected.is_some_and(|expected| expected != actual) {
        return Err(ProtocolError::IdentityMismatch(
            "Agent read revision does not match its query constraint".to_owned(),
        ));
    }
    Ok(())
}

/// Revision-pinned exact Session metadata read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionRead {
    /// Exact `StateRoot` revision read by the query.
    pub revision: String,
    /// Current Session metadata, absent when the exact key does not exist.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current: Option<AgentSessionCurrent>,
}

impl AgentSessionRead {
    /// Verify revision pinning and exact Session ownership for one query.
    ///
    /// # Errors
    ///
    /// Returns an error when the query or read revision is invalid, or when the
    /// returned current does not belong to the exact queried Session.
    pub fn verify_for(&self, query: &AgentSessionQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_read_revision(&self.revision, query.expected_revision.as_ref())?;
        if let Some(current) = &self.current {
            current.verify()?;
            if current.session_id != query.session_id {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent Session read changed its exact key".to_owned(),
                ));
            }
        }
        validate_canonical_size("Agent Session read", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Revision-pinned exact message read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMessageRead {
    /// Exact `StateRoot` revision read by the query.
    pub revision: String,
    /// Current immutable message entry, absent when the exact key does not exist.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current: Option<AgentMessageCurrent>,
}

impl AgentMessageRead {
    /// Verify revision pinning and exact Session/message ownership for one query.
    ///
    /// # Errors
    ///
    /// Returns an error when the query or read revision is invalid, or when the
    /// returned message does not match the exact queried key.
    pub fn verify_for(&self, query: &AgentMessageQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_read_revision(&self.revision, query.expected_revision.as_ref())?;
        if let Some(current) = &self.current {
            current.verify()?;
            if current.session_id != query.session_id
                || current.message.message_id != query.message_id
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent message read changed its exact key".to_owned(),
                ));
            }
        }
        validate_canonical_size("Agent message read", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Keyed current projection for one tool call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentToolCurrent {
    /// Owning Session identity.
    pub session_id: String,
    /// Complete current tool projection.
    pub tool: ToolCall,
    /// Exact Agent command that produced this current value.
    pub admitted_by: String,
}

impl AgentToolCurrent {
    /// Verify the bounded keyed tool projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the owner, tool projection, admitting command, or
    /// bounded wire size is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        validate_tool_projection(&self.tool)?;
        validate_sha256("Agent tool command", &self.admitted_by)?;
        validate_canonical_size("Agent tool current", self, MAX_AGENT_CURRENT_BYTES)
    }

    /// Return the exact bounded byte charge reserved for deterministic Session close.
    ///
    /// # Errors
    ///
    /// Returns an error when this current is invalid or already terminal.
    pub fn nonterminal_close_charge(&self) -> ProtocolResult<u64> {
        tool_close_charge(self)
    }
}

/// Keyed current projection for one elicitation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentElicitationCurrent {
    /// Owning Session identity.
    pub session_id: String,
    /// Complete current elicitation projection.
    pub elicitation: ElicitationProjection,
    /// Exact Agent command that produced this current value.
    pub admitted_by: String,
}

impl AgentElicitationCurrent {
    /// Verify the bounded keyed elicitation projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the owner, elicitation projection, admitting
    /// command, or bounded wire size is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        self.elicitation.validate()?;
        validate_sha256("Agent elicitation command", &self.admitted_by)?;
        validate_canonical_size("Agent elicitation current", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Keyed idempotency authority for one Session-local update identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentUpdateCurrent {
    /// Owning Session identity.
    pub session_id: String,
    /// Caller-owned idempotency identity.
    pub update_id: String,
    /// Content identity of the complete typed update.
    pub update_digest: String,
    /// Exact Agent command which first admitted this identity.
    pub admitted_by: String,
}

impl AgentUpdateCurrent {
    fn new(session_id: &str, command_id: &str, update: &AgentUpdate) -> ProtocolResult<Self> {
        let current = Self {
            session_id: session_id.to_owned(),
            update_id: update.update_id().to_owned(),
            update_digest: content_id(AGENT_UPDATE_DIGEST_DOMAIN, update)?,
            admitted_by: command_id.to_owned(),
        };
        current.verify_for(update)?;
        Ok(current)
    }

    /// Verify owner, update identity, complete content digest, and admitting command.
    ///
    /// # Errors
    ///
    /// Returns an error when the keyed authority does not exactly match the
    /// supplied update or contains an invalid owner or command identity.
    pub fn verify_for(&self, update: &AgentUpdate) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        validate_identity("Agent update", &self.update_id)?;
        validate_sha256("Agent update command", &self.admitted_by)?;
        if self.update_id != update.update_id()
            || self.update_digest != content_id(AGENT_UPDATE_DIGEST_DOMAIN, update)?
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent update current does not match its complete update content".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Exact keyed entry resolved with one Session update command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentSessionEntrySource {
    /// State, Plan, or usage changes need metadata only.
    Metadata,
    /// Complete bounded non-terminal Tool set used only by Session closure.
    Close {
        /// Exact independently keyed currents in Tool-identity order.
        tools: Vec<AgentToolCurrent>,
    },
    /// Existing message alias and ordinal entry, if any.
    Message {
        /// Current immutable message alias.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        current: Option<AgentMessageCurrent>,
    },
    /// Existing tool current, if any.
    Tool {
        /// Current tool projection.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        current: Option<AgentToolCurrent>,
    },
}

/// Bounded exact before witness for one direct Session update.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionUpdateSource {
    /// Existing update identity; terminal admission requires it to be absent.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub update: Option<AgentUpdateCurrent>,
    /// Exact keyed domain entry affected by the update.
    pub entry: AgentSessionEntrySource,
    /// Exact target-claim sources affected by this update, ordered by claim key.
    pub target_claims: Vec<AgentTargetClaimSource>,
}

/// Bounded exact postcondition of one Session update.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentSessionUpdateEffect {
    /// Only bounded Session metadata changed.
    Metadata,
    /// Session closure plus every deterministic Tool cancellation.
    Closed {
        /// Exact Cancelled successors in Tool-identity order.
        tools: Vec<AgentToolCurrent>,
    },
    /// One immutable message alias and order entry were admitted or replayed.
    Message {
        /// Exact current message authority.
        current: AgentMessageCurrent,
    },
    /// One tool current advanced.
    Tool {
        /// Exact current tool authority.
        current: AgentToolCurrent,
    },
}

/// Bounded Session metadata plus the only keyed entry affected by an update.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSessionPostcondition {
    /// Exact bounded Session metadata after the update.
    pub session: AgentSessionCurrent,
    /// Newly admitted update identity.
    pub update: AgentUpdateCurrent,
    /// Shape-matched exact affected entry.
    pub effect: AgentSessionUpdateEffect,
}

impl AgentSessionCurrent {
    /// Deterministically reduce one direct Session update over its exact keyed source.
    ///
    /// # Errors
    ///
    /// Returns an error when the source does not exactly match current state, the
    /// update was already admitted, the transition is illegal, or the derived
    /// bounded postcondition is invalid.
    pub fn reduce_update(
        &self,
        command_id: &str,
        update: &AgentUpdate,
        source: &AgentSessionUpdateSource,
    ) -> ProtocolResult<AgentSessionPostcondition> {
        let mut session = prepare_session_update(self, command_id, update, source)?;
        let effect = match update {
            AgentUpdate::Message { message, .. } => {
                reduce_message_update(&mut session, command_id, message, &source.entry)?
            }
            AgentUpdate::State {
                state, stop_reason, ..
            } => reduce_state_update(
                &mut session,
                command_id,
                *state,
                *stop_reason,
                &source.entry,
            )?,
            AgentUpdate::Plan { plan, .. } => {
                reduce_plan_update(&mut session, plan, &source.entry)?
            }
            AgentUpdate::Tool { tool, .. } => {
                reduce_tool_update(&mut session, command_id, tool, &source.entry)?
            }
            AgentUpdate::Usage { usage, .. } => {
                reduce_usage_update(&mut session, usage, &source.entry)?
            }
            AgentUpdate::Elicitation { .. } => {
                return Err(ProtocolError::Validation(
                    "Agent elicitation mutation is owned exclusively by AgentInputCommand"
                        .to_owned(),
                ));
            }
        };
        session.verify()?;
        let postcondition = AgentSessionPostcondition {
            update: AgentUpdateCurrent::new(&session.session_id, command_id, update)?,
            session,
            effect,
        };
        postcondition.verify_for(update)?;
        let _ = reduce_agent_session_target_claims(command_id, update, source, &postcondition)?;
        Ok(postcondition)
    }
}

fn prepare_session_update(
    current: &AgentSessionCurrent,
    command_id: &str,
    update: &AgentUpdate,
    source: &AgentSessionUpdateSource,
) -> ProtocolResult<AgentSessionCurrent> {
    current.verify()?;
    validate_sha256("Agent Session command", command_id)?;
    update.validate_content()?;
    if let Some(admitted) = &source.update {
        admitted.verify_for(update)?;
        return Err(ProtocolError::IllegalTransition(format!(
            "Agent update {} already has an admitting command",
            update.update_id()
        )));
    }
    if current.state == AgentState::Closed {
        return Err(ProtocolError::IllegalTransition(
            "closed Agent Session cannot accept another update".to_owned(),
        ));
    }
    let mut session = current.clone();
    session.last_transition = Some(AgentSessionTransitionWitness {
        command_id: command_id.to_owned(),
        kind: AgentSessionTransitionKind::SessionUpdate,
    });
    if let Some(sequence) = update_numeric_sequence(update.update_id()) {
        session.latest_update_sequence = session.latest_update_sequence.max(sequence);
    }
    Ok(session)
}

fn require_metadata_entry(entry: &AgentSessionEntrySource) -> ProtocolResult<()> {
    if !matches!(entry, AgentSessionEntrySource::Metadata) {
        return Err(ProtocolError::Validation(
            "Agent Session update source does not match its typed update".to_owned(),
        ));
    }
    Ok(())
}

fn reduce_message_update(
    session: &mut AgentSessionCurrent,
    command_id: &str,
    message: &AgentMessage,
    entry: &AgentSessionEntrySource,
) -> ProtocolResult<AgentSessionUpdateEffect> {
    let AgentSessionEntrySource::Message { current } = entry else {
        return Err(ProtocolError::Validation(
            "Agent Session update source does not match its typed update".to_owned(),
        ));
    };
    if let Some(current) = current {
        current.verify()?;
        return Err(ProtocolError::IllegalTransition(format!(
            "message {} already has immutable content in Session {}",
            message.message_id, session.session_id
        )));
    }
    let order = AgentMessageOrderEntry::new(session, command_id, message)?;
    session.message_count = session
        .message_count
        .checked_add(1)
        .ok_or_else(|| ProtocolError::Validation("Agent message count is exhausted".to_owned()))?;
    session.message_head = Some(order.head.clone());
    Ok(AgentSessionUpdateEffect::Message {
        current: AgentMessageCurrent {
            session_id: session.session_id.clone(),
            message: message.clone(),
            order,
        },
    })
}

fn reduce_state_update(
    session: &mut AgentSessionCurrent,
    command_id: &str,
    state: AgentState,
    stop_reason: Option<SessionStopReason>,
    entry: &AgentSessionEntrySource,
) -> ProtocolResult<AgentSessionUpdateEffect> {
    if state == AgentState::Closed {
        return reduce_session_close(session, command_id, stop_reason, entry);
    }
    require_metadata_entry(entry)?;
    if session.state == state && session.stop_reason == stop_reason {
        return Err(ProtocolError::IllegalTransition(
            "Agent Session state update cannot be a new-command no-op".to_owned(),
        ));
    }
    session.state = state;
    session.stop_reason = stop_reason;
    Ok(AgentSessionUpdateEffect::Metadata)
}

fn reduce_session_close(
    session: &mut AgentSessionCurrent,
    command_id: &str,
    stop_reason: Option<SessionStopReason>,
    entry: &AgentSessionEntrySource,
) -> ProtocolResult<AgentSessionUpdateEffect> {
    let AgentSessionEntrySource::Close { tools } = entry else {
        return Err(ProtocolError::Validation(
            "Agent Session closure requires its complete bounded non-terminal Tool source"
                .to_owned(),
        ));
    };
    if stop_reason.is_some() {
        return Err(ProtocolError::Validation(
            "closed Agent Session cannot carry a stop reason".to_owned(),
        ));
    }
    if tools.len() != session.nonterminal_tools.len() {
        return Err(ProtocolError::IdentityMismatch(
            "Agent Session close source does not contain every non-terminal Tool".to_owned(),
        ));
    }
    let mut previous_tool_id: Option<&str> = None;
    let mut cancelled = Vec::with_capacity(tools.len());
    for current in tools {
        current.verify()?;
        let tool_call_id = current.tool.tool_call_id.as_str();
        if current.session_id != session.session_id
            || current.tool.status.is_terminal()
            || previous_tool_id.is_some_and(|previous| previous >= tool_call_id)
            || session
                .nonterminal_tools
                .get(tool_call_id)
                .is_none_or(|entry| entry.verify_for(current).is_err())
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent Session close source changed its bounded non-terminal Tool set".to_owned(),
            ));
        }
        previous_tool_id = Some(tool_call_id);
        cancelled.push(cancelled_tool_current(current, command_id)?);
    }
    session.state = AgentState::Closed;
    session.stop_reason = None;
    session.nonterminal_tools.clear();
    Ok(AgentSessionUpdateEffect::Closed { tools: cancelled })
}

fn reduce_plan_update(
    session: &mut AgentSessionCurrent,
    plan: &AgentPlan,
    entry: &AgentSessionEntrySource,
) -> ProtocolResult<AgentSessionUpdateEffect> {
    require_metadata_entry(entry)?;
    if session.plan.as_ref() == Some(plan) {
        return Err(ProtocolError::IllegalTransition(
            "Agent Session Plan update cannot be a new-command no-op".to_owned(),
        ));
    }
    session.plan = Some(plan.clone());
    Ok(AgentSessionUpdateEffect::Metadata)
}

fn reduce_usage_update(
    session: &mut AgentSessionCurrent,
    usage: &Usage,
    entry: &AgentSessionEntrySource,
) -> ProtocolResult<AgentSessionUpdateEffect> {
    require_metadata_entry(entry)?;
    if session.usage.as_ref() == Some(usage) {
        return Err(ProtocolError::IllegalTransition(
            "Agent Session usage update cannot be a new-command no-op".to_owned(),
        ));
    }
    session.usage = Some(usage.clone());
    Ok(AgentSessionUpdateEffect::Metadata)
}

fn reduce_tool_update(
    session: &mut AgentSessionCurrent,
    command_id: &str,
    tool: &ToolCall,
    entry: &AgentSessionEntrySource,
) -> ProtocolResult<AgentSessionUpdateEffect> {
    let AgentSessionEntrySource::Tool { current } = entry else {
        return Err(ProtocolError::Validation(
            "Agent Session update source does not match its typed update".to_owned(),
        ));
    };
    if let Some(current) = current {
        current.verify()?;
        if current.session_id != session.session_id
            || current.tool.tool_call_id != tool.tool_call_id
            || current.tool.operation != tool.operation
            || current.tool.input != tool.input
            || current.tool.locations != tool.locations
            || current.tool.status == tool.status
            || current.tool.status.is_terminal()
            || !valid_tool_transition(current.tool.status, tool.status)
        {
            return Err(ProtocolError::IllegalTransition(format!(
                "tool {} cannot advance from its retained projection",
                tool.tool_call_id
            )));
        }
    } else if tool.status != ToolCallStatus::Pending {
        return Err(ProtocolError::IllegalTransition(format!(
            "tool {} must enter the Session as pending",
            tool.tool_call_id
        )));
    }
    validate_tool_projection(tool)?;
    let next = AgentToolCurrent {
        session_id: session.session_id.clone(),
        tool: tool.clone(),
        admitted_by: command_id.to_owned(),
    };
    advance_nonterminal_tool_directory(session, current.as_ref(), &next)?;
    Ok(AgentSessionUpdateEffect::Tool { current: next })
}

fn exact_target_claim_sources<'a>(
    session_id: &str,
    sources: &'a [AgentTargetClaimSource],
    expected: &[AgentTargetClaimTarget],
) -> ProtocolResult<Vec<&'a AgentTargetClaimSource>> {
    if sources.len() != expected.len() {
        return Err(ProtocolError::IdentityMismatch(
            "Agent Session update changed its exact target-claim source set".to_owned(),
        ));
    }
    let mut previous_key: Option<String> = None;
    let mut selected = Vec::with_capacity(sources.len());
    for (source, expected_target) in sources.iter().zip(expected) {
        source.verify_for(session_id)?;
        let key = agent_target_claim_key(session_id, &source.target)?;
        if source.target != *expected_target
            || previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &key)
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent Session update target-claim sources changed key or order".to_owned(),
            ));
        }
        previous_key = Some(key);
        selected.push(source);
    }
    Ok(selected)
}

fn require_unclaimed_target(source: &AgentTargetClaimSource) -> ProtocolResult<()> {
    if source
        .current
        .as_ref()
        .is_some_and(|current| !matches!(current.phase, AgentTargetClaimPhase::Released { .. }))
    {
        return Err(ProtocolError::Conflict {
            code: "agent_target_already_claimed".to_owned(),
            message: "Agent target is reserved or already materialized".to_owned(),
        });
    }
    Ok(())
}

fn materialize_target_claim(
    session_id: &str,
    target: AgentTargetClaimTarget,
    source: &AgentTargetClaimSource,
    command_id: &str,
) -> ProtocolResult<AgentTargetClaimTransition> {
    match source.current.as_ref().map(|current| &current.phase) {
        Some(AgentTargetClaimPhase::Reserved { .. })
            if source
                .current
                .as_ref()
                .is_some_and(|current| current.admitted_by == command_id) => {}
        _ => require_unclaimed_target(source)?,
    }
    AgentTargetClaimTransition::new(
        session_id,
        target,
        source.current.as_ref(),
        AgentTargetClaimPhase::Materialized,
        command_id,
    )
}

fn reduce_agent_session_target_claims(
    command_id: &str,
    update: &AgentUpdate,
    source: &AgentSessionUpdateSource,
    postcondition: &AgentSessionPostcondition,
) -> ProtocolResult<Vec<AgentTargetClaimTransition>> {
    let session_id = postcondition.session.session_id.as_str();
    match (update, &postcondition.effect) {
        (AgentUpdate::Message { message, .. }, AgentSessionUpdateEffect::Message { current }) => {
            let target = AgentTargetClaimTarget::Message {
                message_id: message.message_id.clone(),
            };
            if current.session_id != session_id || current.message != *message {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent Message postcondition changed its target claim".to_owned(),
                ));
            }
            let claims = exact_target_claim_sources(
                session_id,
                &source.target_claims,
                std::slice::from_ref(&target),
            )?;
            Ok(vec![materialize_target_claim(
                session_id, target, claims[0], command_id,
            )?])
        }
        (AgentUpdate::Tool { tool, .. }, AgentSessionUpdateEffect::Tool { current }) => {
            let target = AgentTargetClaimTarget::Tool {
                tool_call_id: tool.tool_call_id.clone(),
            };
            if current.session_id != session_id || current.tool != *tool {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent Tool postcondition changed its target claim".to_owned(),
                ));
            }
            let claims = exact_target_claim_sources(
                session_id,
                &source.target_claims,
                std::slice::from_ref(&target),
            )?;
            if tool.status.is_terminal() {
                Ok(vec![materialize_target_claim(
                    session_id, target, claims[0], command_id,
                )?])
            } else {
                require_unclaimed_target(claims[0])?;
                Ok(Vec::new())
            }
        }
        (
            AgentUpdate::State {
                state: AgentState::Closed,
                ..
            },
            AgentSessionUpdateEffect::Closed { tools },
        ) => {
            let mut targets = tools
                .iter()
                .map(|current| {
                    let target = AgentTargetClaimTarget::Tool {
                        tool_call_id: current.tool.tool_call_id.clone(),
                    };
                    Ok((
                        agent_target_claim_key(session_id, &target)?,
                        target,
                        current,
                    ))
                })
                .collect::<ProtocolResult<Vec<_>>>()?;
            targets.sort_by(|left, right| left.0.cmp(&right.0));
            let expected = targets
                .iter()
                .map(|(_, target, _)| target.clone())
                .collect::<Vec<_>>();
            let claims = exact_target_claim_sources(session_id, &source.target_claims, &expected)?;
            targets
                .into_iter()
                .zip(claims)
                .map(|((_, target, current), source)| {
                    if current.session_id != session_id
                        || current.tool.status != ToolCallStatus::Cancelled
                    {
                        return Err(ProtocolError::IdentityMismatch(
                            "Agent Session close changed its cancelled target".to_owned(),
                        ));
                    }
                    materialize_target_claim(session_id, target, source, command_id)
                })
                .collect()
        }
        (
            AgentUpdate::State { .. } | AgentUpdate::Plan { .. } | AgentUpdate::Usage { .. },
            AgentSessionUpdateEffect::Metadata,
        ) => {
            exact_target_claim_sources(session_id, &source.target_claims, &[])?;
            Ok(Vec::new())
        }
        _ => Err(ProtocolError::IdentityMismatch(
            "Agent Session update target claims do not match its typed postcondition".to_owned(),
        )),
    }
}

/// Derive the exact independent target-claim mutations owned by one Session
/// command receipt.
///
/// # Errors
///
/// Returns an error when command, source, postcondition, target source, or
/// closed claim transition differs from the unique Session reducer result.
pub fn agent_session_target_claim_transitions(
    command: &AgentCommand,
    session: &AgentSessionCurrent,
    source: &AgentSessionUpdateSource,
    postcondition: &AgentSessionPostcondition,
) -> ProtocolResult<Vec<AgentTargetClaimTransition>> {
    command.verify()?;
    let AgentCommandAction::SessionUpdate { session_id, update } = &command.action else {
        return Err(ProtocolError::Validation(
            "Agent Session target claims require a SessionUpdate command".to_owned(),
        ));
    };
    if postcondition.session.session_id != *session_id {
        return Err(ProtocolError::IdentityMismatch(
            "Agent Session target claims changed their Session owner".to_owned(),
        ));
    }
    let expected = session.reduce_update(&command.command_id, update, source)?;
    if expected != *postcondition {
        return Err(ProtocolError::IdentityMismatch(
            "Agent Session target claims changed the authorized postcondition".to_owned(),
        ));
    }
    reduce_agent_session_target_claims(&command.command_id, update, source, postcondition)
}

fn advance_nonterminal_tool_directory(
    session: &mut AgentSessionCurrent,
    previous: Option<&AgentToolCurrent>,
    next: &AgentToolCurrent,
) -> ProtocolResult<()> {
    let tool_call_id = next.tool.tool_call_id.as_str();
    match previous {
        None => {
            if session.nonterminal_tools.contains_key(tool_call_id)
                || next.tool.status.is_terminal()
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent non-terminal Tool directory does not match first admission".to_owned(),
                ));
            }
        }
        Some(previous) => {
            if session
                .nonterminal_tools
                .get(tool_call_id)
                .is_none_or(|entry| entry.verify_for(previous).is_err())
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent non-terminal Tool directory does not match its exact current".to_owned(),
                ));
            }
        }
    }
    if next.tool.status.is_terminal() {
        session.nonterminal_tools.remove(tool_call_id);
    } else {
        session
            .nonterminal_tools
            .insert(tool_call_id.to_owned(), AgentNonterminalTool::new(next)?);
    }
    session.verify()
}

fn cancelled_tool_current(
    current: &AgentToolCurrent,
    command_id: &str,
) -> ProtocolResult<AgentToolCurrent> {
    validate_sha256("Agent Tool cancellation command", command_id)?;
    let mut tool = current.tool.clone();
    tool.status = ToolCallStatus::Cancelled;
    tool.output = None;
    let cancelled = AgentToolCurrent {
        session_id: current.session_id.clone(),
        tool,
        admitted_by: command_id.to_owned(),
    };
    cancelled.verify()?;
    Ok(cancelled)
}

fn tool_close_charge(current: &AgentToolCurrent) -> ProtocolResult<u64> {
    current.verify()?;
    if current.tool.status.is_terminal() {
        return Err(ProtocolError::IllegalTransition(
            "terminal Agent Tool cannot retain Session-close capacity".to_owned(),
        ));
    }
    let cancelled = cancelled_tool_current(current, &current.admitted_by)?;
    let bytes = cymule_core::canonical_bytes(current)?
        .len()
        .checked_add(cymule_core::canonical_bytes(&cancelled)?.len())
        .ok_or_else(|| {
            ProtocolError::Validation("Agent Tool close byte charge is exhausted".to_owned())
        })?;
    u64::try_from(bytes).map_err(|_| {
        ProtocolError::Validation("Agent Tool close byte charge exceeds u64".to_owned())
    })
}

impl AgentSessionPostcondition {
    /// Verify bounded postcondition shape and exact affected value for one update.
    ///
    /// # Errors
    ///
    /// Returns an error when the postcondition is invalid or its Session,
    /// update authority, and affected entry do not exactly encode the update.
    pub fn verify_for(&self, update: &AgentUpdate) -> ProtocolResult<()> {
        self.session.verify()?;
        self.update.verify_for(update)?;
        if self.update.session_id != self.session.session_id {
            return Err(ProtocolError::IdentityMismatch(
                "Agent update current escaped its Session postcondition".to_owned(),
            ));
        }
        match (update, &self.effect) {
            (
                AgentUpdate::Message { message, .. },
                AgentSessionUpdateEffect::Message { current },
            ) if current.session_id == self.session.session_id
                && current.message == *message
                && self.session.message_head.as_ref() == Some(&current.order.head)
                && current.order.index.checked_add(1) == Some(self.session.message_count) =>
            {
                current.verify()
            }
            (
                AgentUpdate::State {
                    state, stop_reason, ..
                },
                AgentSessionUpdateEffect::Metadata,
            ) if *state != AgentState::Closed
                && self.session.state == *state
                && self.session.stop_reason == *stop_reason =>
            {
                Ok(())
            }
            (
                AgentUpdate::State {
                    state: AgentState::Closed,
                    stop_reason: None,
                    ..
                },
                AgentSessionUpdateEffect::Closed { tools },
            ) if self.session.state == AgentState::Closed
                && self.session.stop_reason.is_none()
                && self.session.nonterminal_tools.is_empty()
                && tools.iter().all(|current| {
                    current.session_id == self.session.session_id
                        && current.tool.status == ToolCallStatus::Cancelled
                }) =>
            {
                tools.iter().try_for_each(AgentToolCurrent::verify)
            }
            (AgentUpdate::Plan { plan, .. }, AgentSessionUpdateEffect::Metadata)
                if self.session.plan.as_ref() == Some(plan) =>
            {
                Ok(())
            }
            (AgentUpdate::Usage { usage, .. }, AgentSessionUpdateEffect::Metadata)
                if self.session.usage.as_ref() == Some(usage) =>
            {
                Ok(())
            }
            (AgentUpdate::Tool { tool, .. }, AgentSessionUpdateEffect::Tool { current })
                if current.session_id == self.session.session_id && current.tool == *tool =>
            {
                current.verify()
            }
            (AgentUpdate::Elicitation { .. }, _) => Err(ProtocolError::Validation(
                "Agent elicitation mutation is owned exclusively by AgentInputCommand".to_owned(),
            )),
            _ => Err(ProtocolError::IdentityMismatch(
                "Agent Session postcondition does not match its exact update".to_owned(),
            )),
        }
    }
}

fn update_numeric_sequence(update_id: &str) -> Option<u64> {
    update_id.rsplit(':').next()?.parse().ok()
}

fn valid_tool_transition(previous: ToolCallStatus, next: ToolCallStatus) -> bool {
    matches!(
        (previous, next),
        (
            ToolCallStatus::Pending,
            ToolCallStatus::AwaitingPermission | ToolCallStatus::InProgress
        ) | (
            ToolCallStatus::AwaitingPermission,
            ToolCallStatus::InProgress | ToolCallStatus::Cancelled
        ) | (
            ToolCallStatus::InProgress,
            ToolCallStatus::Completed | ToolCallStatus::Failed | ToolCallStatus::Cancelled
        )
    )
}

impl ToolCallStatus {
    const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

fn validate_tool_projection(tool: &ToolCall) -> ProtocolResult<()> {
    validate_content_token("tool identity", &tool.tool_call_id, 512)?;
    validate_content_token("tool operation", &tool.operation, 512)?;
    if !tool.status.is_terminal() && tool.output.is_some() {
        return Err(ProtocolError::Validation(format!(
            "non-terminal tool {} cannot publish output",
            tool.tool_call_id
        )));
    }
    if let Some(output) = &tool.output {
        validate_content_blocks(output)?;
    }
    validate_canonical_size("Agent tool current", tool, MAX_AGENT_VALUE_BYTES)
}

fn validate_agent_plan(plan: &AgentPlan) -> ProtocolResult<()> {
    validate_content_token("Plan identity", &plan.plan_id, 512)?;
    validate_count(
        "Agent Plan entries",
        plan.entries.len(),
        MAX_AGENT_VALUE_ENTRIES,
    )?;
    let mut entry_ids = BTreeSet::new();
    for entry in &plan.entries {
        validate_content_token("Plan entry identity", &entry.entry_id, 512)?;
        validate_content_token("Plan entry content", &entry.content, 16 * 1024)?;
        if !entry_ids.insert(entry.entry_id.as_str()) {
            return Err(ProtocolError::Validation(format!(
                "Agent Plan repeats entry {}",
                entry.entry_id
            )));
        }
    }
    validate_canonical_size("Agent Plan", plan, MAX_AGENT_VALUE_BYTES)
}

fn validate_usage(usage: &Usage) -> ProtocolResult<()> {
    if usage.used > MAX_EXACT_INTEGER || usage.capacity > MAX_EXACT_INTEGER {
        return Err(ProtocolError::Validation(
            "Agent usage exceeds the shared exact-integer range".to_owned(),
        ));
    }
    if usage.cost.as_ref().is_some_and(Value::is_null) {
        return Err(ProtocolError::Validation(
            "Agent usage null cost must use the absent representation".to_owned(),
        ));
    }
    validate_canonical_size("Agent usage", usage, MAX_AGENT_VALUE_BYTES)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Hard cumulative limits for one pinned context-selection scan.
pub struct AgentContextScanLimits {
    /// Maximum ordered message entries the adapter may inspect.
    pub max_entries: u64,
    /// Maximum canonical message bytes the adapter may inspect.
    pub max_canonical_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Request for an adapter-selected bounded context snapshot.
pub struct ContextRequest {
    /// Session whose immutable message order is pinned.
    pub session_id: String,
    /// Exact message-order head pinned for the complete selection capability.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub source_message_head: Option<String>,
    /// Exact number of messages in the immutable source prefix.
    pub source_message_count: u64,
    /// Caller-defined bounded selection budget.
    pub budget: u64,
    /// Framework-enforced cumulative scan limits which cursors cannot reset.
    pub scan_limits: AgentContextScanLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable ordered-message binding selected into one context snapshot.
pub struct AgentContextMessageRef {
    /// Stable zero-based Session message ordinal.
    pub index: u64,
    /// Stable message identity.
    pub message_id: String,
    /// Complete immutable message payload digest.
    pub message_digest: String,
}

impl AgentContextMessageRef {
    /// Derive the only context-selection reference represented by one exact
    /// verified persisted message current.
    ///
    /// # Errors
    ///
    /// Returns an error when the persisted current is invalid.
    pub fn from_current(current: &AgentMessageCurrent) -> ProtocolResult<Self> {
        current.verify()?;
        Ok(Self {
            index: current.order.index,
            message_id: current.message.message_id.clone(),
            message_digest: current.order.message_digest.clone(),
        })
    }

    /// Verify this selected binding against one exact persisted message current.
    ///
    /// # Errors
    ///
    /// Returns an error when the current is invalid or its Session ordinal,
    /// message identity, or immutable payload digest differs from this binding.
    pub fn verify_for(&self, current: &AgentMessageCurrent) -> ProtocolResult<()> {
        current.verify()?;
        if self.index != current.order.index
            || self.message_id != current.message.message_id
            || self.message_digest != current.order.message_digest
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent context message binding does not match its persisted current".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable context selected for one model occurrence.
pub struct ContextSnapshot {
    /// Stable snapshot identity.
    pub snapshot_id: String,
    /// Exact message-order head over which selection ran.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub source_message_head: Option<String>,
    /// Exact number of messages in the immutable source prefix.
    pub source_message_count: u64,
    /// Ordered immutable message bindings used to derive `content`.
    pub selected_messages: Vec<AgentContextMessageRef>,
    /// Selected content.
    pub content: Vec<ContentBlock>,
    /// Pinned context-adapter binding.
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One model-host request over a pinned context snapshot.
pub struct ModelRequest {
    /// Owning Session identity.
    pub session_id: String,
    /// Exact context visible to this occurrence.
    pub context: ContextSnapshot,
    /// Abstract tool operations offered by the caller.
    pub tools: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Final response from one model occurrence.
pub struct ModelResponse {
    /// Finalized Agent message.
    pub message: AgentMessage,
    /// Tool requests returned to the caller-owned loop.
    pub tool_requests: Vec<ToolRequest>,
    /// Pinned model-adapter binding.
    pub occurrence_binding: String,
    /// Cumulative or occurrence usage observation.
    pub usage: Usage,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Separate authorization request for one proposed tool call.
pub struct PermissionRequest {
    /// Stable authorization request identity.
    pub request_id: String,
    /// Tool request being authorized.
    pub tool: ToolRequest,
    /// Closed decisions presented to the policy or user.
    pub options: Vec<PermissionDecision>,
}
/// Permission outcome kept separate from tool availability and execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecision {
    /// Permit this occurrence only.
    AllowOnce,
    /// Refuse this occurrence.
    Deny,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One pinned permission decision.
pub struct PermissionResponse {
    /// Explicit authorization outcome.
    pub decision: PermissionDecision,
    /// Pinned permission-adapter binding.
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Proposed invocation of an abstract tool operation.
pub struct ToolRequest {
    /// Stable tool-call identity.
    pub tool_call_id: String,
    /// Abstract operation name.
    pub operation: String,
    /// Structured immutable input.
    pub input: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Final response from one tool occurrence.
pub struct ToolResponse {
    /// Matching tool-call identity.
    pub tool_call_id: String,
    /// Finalized output content.
    pub content: Vec<ContentBlock>,
    /// Pinned tool-adapter binding.
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Typed request for human or external input.
pub struct ElicitationRequest {
    /// Stable input request identity.
    pub request_id: String,
    /// Self-contained JSON Schema for an accepted value.
    pub schema: Value,
    /// User-visible prompt content.
    pub prompt: Vec<ContentBlock>,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
/// Accepted or declined elicitation result.
pub struct ElicitationResponse {
    /// Matching input request identity.
    pub request_id: String,
    /// Whether the responder accepted the request.
    pub accepted: bool,
    /// Validated value when accepted; absent when declined.
    pub value: Option<Value>,
    /// Pinned elicitation-adapter binding.
    pub occurrence_binding: String,
}

impl<'de> Deserialize<'de> for ElicitationResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            request_id: String,
            accepted: bool,
            value: Value,
            occurrence_binding: String,
        }

        let wire = Wire::deserialize(deserializer)?;
        let value = if wire.accepted {
            Some(wire.value)
        } else if wire.value.is_null() {
            None
        } else {
            return Err(serde::de::Error::custom(
                "declined elicitation response must carry an explicit null value",
            ));
        };
        Ok(Self {
            request_id: wire.request_id,
            accepted: wire.accepted,
            value,
            occurrence_binding: wire.occurrence_binding,
        })
    }
}

impl ElicitationResponse {
    fn validate(&self) -> ProtocolResult<()> {
        validate_content_token("elicitation response identity", &self.request_id, 512)?;
        validate_content_token(
            "elicitation occurrence binding",
            &self.occurrence_binding,
            512,
        )?;
        if self.accepted != self.value.is_some() {
            return Err(ProtocolError::Validation(
                "accepted elicitation requires a value and declined elicitation forbids one"
                    .to_owned(),
            ));
        }
        validate_canonical_size("Agent elicitation response", self, MAX_AGENT_VALUE_BYTES)
    }
}

/// Durable projection of one typed input request and optional completion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElicitationProjection {
    /// Owning M1 wait identity.
    pub wait_id: String,
    /// Immutable input request.
    pub request: ElicitationRequest,
    /// Optional terminal response.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub response: Option<ElicitationResponse>,
}

impl ElicitationProjection {
    /// Validate immutable request identity and completion shape.
    ///
    /// # Errors
    ///
    /// Returns an error when the wait identity, request schema, prompt,
    /// response, or bounded representation is invalid.
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_content_token("elicitation wait identity", &self.wait_id, 512)?;
        validate_elicitation_request(&self.request)?;
        if let Some(response) = &self.response {
            validate_elicitation_response(&self.request, response)?;
        }
        validate_canonical_size("Agent elicitation current", self, MAX_AGENT_VALUE_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Request to commit or abort a prepared workspace overlay.
pub struct WorkspaceChange {
    /// Stable workspace-change identity.
    pub change_id: String,
    /// Immutable overlay Artifact.
    pub overlay: ArtifactRef,
    /// Whether the overlay should commit rather than abort.
    pub commit: bool,
}

impl WorkspaceChange {
    fn validate(&self) -> ProtocolResult<()> {
        validate_content_token("workspace change identity", &self.change_id, 512)?;
        self.overlay
            .validate()
            .map_err(|error| ProtocolError::Validation(error.to_string()))
    }
}

/// Complete semantic owner of one M1 workspace host occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceOccurrenceOwner {
    /// Exact semantic Run.
    pub run_id: String,
    /// Exact scope affected by the workspace decision.
    pub scope_id: String,
    /// Exact Plan invocation which owns the Effect site.
    pub invocation_id: String,
    /// Exact Plan site.
    pub site_id: String,
    /// Exact structural occurrence key at that site.
    pub occurrence_key: String,
    /// Exact abstract Effect operation.
    pub operation: String,
    /// Structural Effect intent for commit, absent for abort.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub effect_intent_id: Option<String>,
}

impl WorkspaceOccurrenceOwner {
    fn validate(&self, commit: bool) -> ProtocolResult<()> {
        for (kind, value) in [
            ("workspace Run", self.run_id.as_str()),
            ("workspace scope", self.scope_id.as_str()),
            ("workspace invocation", self.invocation_id.as_str()),
            ("workspace site", self.site_id.as_str()),
            ("workspace occurrence key", self.occurrence_key.as_str()),
            ("workspace operation", self.operation.as_str()),
        ] {
            validate_content_token(kind, value, 512)?;
        }
        if commit != self.effect_intent_id.is_some() {
            return Err(ProtocolError::Validation(
                "workspace commit owner requires exactly one Effect intent".to_owned(),
            ));
        }
        if let Some(intent_id) = &self.effect_intent_id {
            validate_content_token("workspace Effect intent", intent_id, 512)?;
        }
        Ok(())
    }
}

/// Durable request identity for standalone or M1-owned workspace host calls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "authority", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceHostRequest {
    /// A standalone workspace interaction with no M1 semantic claim.
    Standalone {
        /// Provider-facing workspace change.
        change: WorkspaceChange,
    },
    /// A workspace interaction owned by one exact M1 semantic occurrence.
    M1Scope {
        /// Full immutable semantic owner.
        owner: WorkspaceOccurrenceOwner,
        /// Provider-facing workspace change.
        change: WorkspaceChange,
    },
}

impl WorkspaceHostRequest {
    /// Construct a standalone workspace request.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider-facing change is malformed.
    pub fn standalone(change: WorkspaceChange) -> ProtocolResult<Self> {
        let request = Self::Standalone { change };
        request.validate()?;
        Ok(request)
    }

    /// Borrow the provider-facing change.
    pub const fn change(&self) -> &WorkspaceChange {
        match self {
            Self::Standalone { change } | Self::M1Scope { change, .. } => change,
        }
    }

    /// Borrow the complete M1 owner, when present.
    pub const fn m1_owner(&self) -> Option<&WorkspaceOccurrenceOwner> {
        match self {
            Self::Standalone { .. } => None,
            Self::M1Scope { owner, .. } => Some(owner),
        }
    }

    /// Construct a Plan-owned M1 workspace request with its complete owner.
    ///
    /// # Errors
    ///
    /// Returns an error when the semantic owner or provider-facing change is
    /// malformed or the commit intent does not match the owner.
    pub fn m1_scope(
        owner: WorkspaceOccurrenceOwner,
        change: WorkspaceChange,
    ) -> ProtocolResult<Self> {
        let request = Self::M1Scope { owner, change };
        request.validate()?;
        Ok(request)
    }

    fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Standalone { change } => change.validate(),
            Self::M1Scope { owner, change } => {
                change.validate()?;
                owner.validate(change.commit)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Final receipt from a workspace adapter.
pub struct WorkspaceReceipt {
    /// Matching workspace-change identity.
    pub change_id: String,
    /// Observed commit outcome.
    pub committed: bool,
    /// Immutable provider evidence.
    pub evidence: ArtifactRef,
    /// Pinned workspace-adapter binding.
    pub occurrence_binding: String,
}

/// Explicit immutable binding selected by an Agent host adapter before dispatch.
///
/// An Agent host is not a Cymule runtime plugin. The `M1EffectOperation`
/// variant therefore records a verifiable closure over an already-admitted M1
/// execution operation instead of claiming to be that plugin provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "authority", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentHostBinding {
    /// A host binding outside an M1 operation boundary.
    Standalone {
        /// Stable Agent-host implementation identity.
        implementation_id: String,
        /// Host-owned immutable occurrence identity returned by responses.
        binding_id: String,
    },
    /// An Agent-host implementation explicitly closed over one M1 Effect pin.
    M1EffectOperation {
        /// Stable Agent-host implementation identity, distinct from `PluginHost`.
        implementation_id: String,
        /// Content identity of this complete host binding descriptor.
        binding_id: String,
        /// Exact M1 `ExecutionBinding` Artifact retained by the Effect.
        execution_binding: ArtifactRef,
        /// Exact abstract Effect operation.
        operation: String,
        /// Exact M1 operation occurrence binding derived by runtime composition.
        operation_occurrence_binding: String,
    },
}

#[derive(Serialize)]
struct M1EffectHostBindingPreimage<'a> {
    binding_version: &'static str,
    implementation_id: &'a str,
    execution_binding: &'a ArtifactRef,
    operation: &'a str,
    operation_occurrence_binding: &'a str,
}

impl AgentHostBinding {
    /// Construct a host-owned binding which has no M1 operation closure.
    ///
    /// # Errors
    ///
    /// Returns an error when either identity is empty, oversized, or contains a
    /// control character.
    pub fn standalone(
        implementation_id: impl Into<String>,
        binding_id: impl Into<String>,
    ) -> ProtocolResult<Self> {
        let binding = Self::Standalone {
            implementation_id: implementation_id.into(),
            binding_id: binding_id.into(),
        };
        binding.verify()?;
        Ok(binding)
    }

    /// Seal one Agent-host implementation over an exact M1 Effect operation.
    ///
    /// # Errors
    ///
    /// Returns an error when the host identity or M1 closure fields are invalid.
    pub fn m1_effect_operation(
        implementation_id: impl Into<String>,
        execution_binding: ArtifactRef,
        operation: impl Into<String>,
        operation_occurrence_binding: impl Into<String>,
    ) -> ProtocolResult<Self> {
        let implementation_id = implementation_id.into();
        let operation = operation.into();
        let operation_occurrence_binding = operation_occurrence_binding.into();
        validate_host_binding_token("implementation", &implementation_id)?;
        validate_host_binding_token("Effect operation", &operation)?;
        validate_host_binding_token(
            "M1 operation occurrence binding",
            &operation_occurrence_binding,
        )?;
        execution_binding
            .validate()
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        if execution_binding.kind != EXECUTION_BINDING_ARTIFACT_KIND {
            return Err(ProtocolError::Validation(
                "Agent host M1 closure requires an ExecutionBinding Artifact".to_owned(),
            ));
        }
        let binding_id = content_id(
            AGENT_HOST_BINDING_VERSION,
            &M1EffectHostBindingPreimage {
                binding_version: AGENT_HOST_BINDING_VERSION,
                implementation_id: &implementation_id,
                execution_binding: &execution_binding,
                operation: &operation,
                operation_occurrence_binding: &operation_occurrence_binding,
            },
        )
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        let binding = Self::M1EffectOperation {
            implementation_id,
            binding_id,
            execution_binding,
            operation,
            operation_occurrence_binding,
        };
        binding.verify()?;
        Ok(binding)
    }

    /// Stable identity which every terminal host response must echo.
    pub fn binding_id(&self) -> &str {
        match self {
            Self::Standalone { binding_id, .. } | Self::M1EffectOperation { binding_id, .. } => {
                binding_id
            }
        }
    }

    /// Validate this complete host binding descriptor and its content identity.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid fields or a forged M1 closure identity.
    pub fn verify(&self) -> ProtocolResult<()> {
        match self {
            Self::Standalone {
                implementation_id,
                binding_id,
            } => {
                validate_host_binding_token("implementation", implementation_id)?;
                validate_host_binding_token("standalone binding", binding_id)
            }
            Self::M1EffectOperation {
                implementation_id,
                binding_id,
                execution_binding,
                operation,
                operation_occurrence_binding,
            } => {
                validate_host_binding_token("implementation", implementation_id)?;
                validate_host_binding_token("Effect operation", operation)?;
                validate_host_binding_token(
                    "M1 operation occurrence binding",
                    operation_occurrence_binding,
                )?;
                execution_binding
                    .validate()
                    .map_err(|error| ProtocolError::Validation(error.to_string()))?;
                if execution_binding.kind != EXECUTION_BINDING_ARTIFACT_KIND {
                    return Err(ProtocolError::Validation(
                        "Agent host M1 closure requires an ExecutionBinding Artifact".to_owned(),
                    ));
                }
                let expected = content_id(
                    AGENT_HOST_BINDING_VERSION,
                    &M1EffectHostBindingPreimage {
                        binding_version: AGENT_HOST_BINDING_VERSION,
                        implementation_id,
                        execution_binding,
                        operation,
                        operation_occurrence_binding,
                    },
                )
                .map_err(|error| ProtocolError::Validation(error.to_string()))?;
                if binding_id != &expected {
                    return Err(ProtocolError::Validation(
                        "Agent host binding identity does not match its M1 closure".to_owned(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Prove exact equivalence with one M1 Effect operation pin.
    ///
    /// # Errors
    ///
    /// Returns an error for standalone bindings, forged descriptors, or any
    /// difference in the `ExecutionBinding` Artifact, operation, or occurrence
    /// binding selected by runtime composition.
    pub fn verify_m1_effect_operation(
        &self,
        execution_binding: &ArtifactRef,
        operation: &str,
        operation_occurrence_binding: &str,
    ) -> ProtocolResult<()> {
        self.verify()?;
        match self {
            Self::M1EffectOperation {
                execution_binding: retained_execution_binding,
                operation: retained_operation,
                operation_occurrence_binding: retained_occurrence_binding,
                ..
            } if retained_execution_binding == execution_binding
                && retained_operation == operation
                && retained_occurrence_binding == operation_occurrence_binding =>
            {
                Ok(())
            }
            Self::M1EffectOperation { .. } => Err(ProtocolError::Validation(
                "Agent host binding does not close over the exact M1 Effect operation pin"
                    .to_owned(),
            )),
            Self::Standalone { .. } => Err(ProtocolError::Validation(
                "standalone Agent host binding cannot realize an M1 workspace Effect".to_owned(),
            )),
        }
    }

    /// Borrow the exact M1 operation closure retained by this descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error when this is a standalone host binding.
    pub fn m1_effect_operation_closure(&self) -> ProtocolResult<(&ArtifactRef, &str, &str)> {
        self.verify()?;
        match self {
            Self::M1EffectOperation {
                execution_binding,
                operation,
                operation_occurrence_binding,
                ..
            } => Ok((execution_binding, operation, operation_occurrence_binding)),
            Self::Standalone { .. } => Err(ProtocolError::Validation(
                "standalone Agent host binding has no M1 Effect operation closure".to_owned(),
            )),
        }
    }
}

fn validate_host_binding_token(kind: &str, value: &str) -> ProtocolResult<()> {
    if value.is_empty() || value.chars().count() > 512 || value.chars().any(char::is_control) {
        return Err(ProtocolError::Validation(format!(
            "Agent host {kind} must contain 1..=512 non-control characters"
        )));
    }
    Ok(())
}

/// Kind of replaceable host interaction recorded as a durable occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHostCallKind {
    /// Context selection.
    Context,
    /// Model invocation.
    Model,
    /// Permission decision.
    Permission,
    /// Tool invocation.
    Tool,
    /// Human or external input request.
    Elicitation,
    /// Workspace overlay application.
    Workspace,
}

/// Typed request admitted at one agent host occurrence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "request",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AgentHostRequest {
    /// Context-selection request.
    Context(ContextRequest),
    /// Model-invocation request.
    Model(ModelRequest),
    /// Permission-decision request.
    Permission(PermissionRequest),
    /// Tool-invocation request.
    Tool(ToolRequest),
    /// Human or external input request.
    Elicitation(ElicitationRequest),
    /// Workspace overlay request.
    Workspace(WorkspaceHostRequest),
}

impl AgentHostRequest {
    /// Closed request kind used for response matching.
    pub const fn kind(&self) -> AgentHostCallKind {
        match self {
            Self::Context(_) => AgentHostCallKind::Context,
            Self::Model(_) => AgentHostCallKind::Model,
            Self::Permission(_) => AgentHostCallKind::Permission,
            Self::Tool(_) => AgentHostCallKind::Tool,
            Self::Elicitation(_) => AgentHostCallKind::Elicitation,
            Self::Workspace(_) => AgentHostCallKind::Workspace,
        }
    }

    /// Whether this request is owned by an M1 workspace scope.
    pub const fn is_m1_workspace(&self) -> bool {
        matches!(self, Self::Workspace(WorkspaceHostRequest::M1Scope { .. }))
    }

    /// Verify the request and exact owning Session identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the Session identity, request-specific payload,
    /// provider binding, scan budget, or bounded wire representation is invalid.
    pub fn validate_for_session(&self, session_id: &str) -> ProtocolResult<()> {
        validate_content_token("Session identity", session_id, 512)?;
        match self {
            Self::Context(request) => {
                if request.session_id != session_id {
                    return Err(ProtocolError::Validation(
                        "context request belongs to a different Agent Session".to_owned(),
                    ));
                }
                if request.budget > MAX_EXACT_INTEGER {
                    return Err(ProtocolError::Validation(
                        "context budget exceeds the shared exact-integer range".to_owned(),
                    ));
                }
                validate_message_source_descriptor(
                    "context source",
                    request.source_message_head.as_deref(),
                    request.source_message_count,
                )?;
                if request.scan_limits.max_entries == 0
                    || request.scan_limits.max_entries > MAX_AGENT_CONTEXT_SCAN_ENTRIES
                    || request.scan_limits.max_canonical_bytes == 0
                    || request.scan_limits.max_canonical_bytes > MAX_AGENT_CONTEXT_SCAN_BYTES
                {
                    return Err(ProtocolError::Validation(format!(
                        "context scan limits must be within 1..={MAX_AGENT_CONTEXT_SCAN_ENTRIES} entries and 1..={MAX_AGENT_CONTEXT_SCAN_BYTES} bytes"
                    )));
                }
            }
            Self::Model(request) => {
                if request.session_id != session_id {
                    return Err(ProtocolError::Validation(
                        "model request belongs to a different Agent Session".to_owned(),
                    ));
                }
                validate_context_snapshot(&request.context)?;
                let mut tools = BTreeSet::new();
                validate_count(
                    "model tool operations",
                    request.tools.len(),
                    MAX_AGENT_VALUE_ENTRIES,
                )?;
                for tool in &request.tools {
                    validate_content_token("model tool operation", tool, 512)?;
                    if !tools.insert(tool.as_str()) {
                        return Err(ProtocolError::Validation(format!(
                            "model request repeats tool operation {tool}"
                        )));
                    }
                }
            }
            Self::Permission(request) => {
                validate_content_token("permission request identity", &request.request_id, 512)?;
                validate_tool_request(&request.tool)?;
                if request.options.is_empty() {
                    return Err(ProtocolError::Validation(
                        "permission request requires at least one closed option".to_owned(),
                    ));
                }
                validate_count(
                    "permission options",
                    request.options.len(),
                    MAX_AGENT_VALUE_ENTRIES,
                )?;
                let mut options = BTreeSet::new();
                for option in &request.options {
                    if !options.insert(*option) {
                        return Err(ProtocolError::Validation(format!(
                            "permission request repeats decision {option:?}"
                        )));
                    }
                }
            }
            Self::Tool(request) => validate_tool_request(request)?,
            Self::Elicitation(request) => {
                validate_elicitation_request(request)?;
            }
            Self::Workspace(request) => request.validate()?,
        }
        validate_canonical_size("Agent host request", self, MAX_AGENT_VALUE_BYTES)
    }
}

/// Typed response durably retained for exact host-call replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "response",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AgentHostResponse {
    /// Context-selection response.
    Context(ContextSnapshot),
    /// Model-invocation response.
    Model(ModelResponse),
    /// Permission-decision response.
    Permission(PermissionResponse),
    /// Tool-invocation response.
    Tool(ToolResponse),
    /// Human or external input response.
    Elicitation(ElicitationResponse),
    /// Workspace overlay response.
    Workspace(WorkspaceReceipt),
}

/// Query-only reconciliation observation for one original host occurrence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "resolution", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentOccurrenceResolution {
    /// The original occurrence completed and returned this typed response.
    Completed {
        /// Original typed host response.
        response: AgentHostResponse,
    },
    /// The original occurrence definitely did not apply.
    NotApplied {
        /// Provider evidence that dispatch did not apply.
        evidence: Vec<ContentBlock>,
    },
    /// The provider still cannot determine the original outcome.
    Unknown {
        /// Evidence available while the result remains ambiguous.
        evidence: Vec<ContentBlock>,
    },
}

impl AgentHostResponse {
    /// Closed response kind used for request matching.
    pub const fn kind(&self) -> AgentHostCallKind {
        match self {
            Self::Context(_) => AgentHostCallKind::Context,
            Self::Model(_) => AgentHostCallKind::Model,
            Self::Permission(_) => AgentHostCallKind::Permission,
            Self::Tool(_) => AgentHostCallKind::Tool,
            Self::Elicitation(_) => AgentHostCallKind::Elicitation,
            Self::Workspace(_) => AgentHostCallKind::Workspace,
        }
    }

    /// Immutable implementation binding returned by the host adapter.
    pub fn occurrence_binding(&self) -> &str {
        match self {
            Self::Context(response) => &response.occurrence_binding,
            Self::Model(response) => &response.occurrence_binding,
            Self::Permission(response) => &response.occurrence_binding,
            Self::Tool(response) => &response.occurrence_binding,
            Self::Elicitation(response) => &response.occurrence_binding,
            Self::Workspace(response) => &response.occurrence_binding,
        }
    }

    fn validate_content(&self) -> ProtocolResult<()> {
        match self {
            Self::Context(response) => validate_context_snapshot(response)?,
            Self::Model(response) => {
                validate_message(&response.message)?;
                validate_content_token(
                    "model occurrence binding",
                    &response.occurrence_binding,
                    512,
                )?;
                let mut tool_ids = BTreeSet::new();
                validate_count(
                    "model tool requests",
                    response.tool_requests.len(),
                    MAX_AGENT_VALUE_ENTRIES,
                )?;
                for tool in &response.tool_requests {
                    validate_tool_request(tool)?;
                    if !tool_ids.insert(tool.tool_call_id.as_str()) {
                        return Err(ProtocolError::Validation(format!(
                            "model response repeats tool identity {}",
                            tool.tool_call_id
                        )));
                    }
                }
                validate_usage(&response.usage)?;
            }
            Self::Permission(response) => validate_content_token(
                "permission occurrence binding",
                &response.occurrence_binding,
                512,
            )?,
            Self::Tool(response) => {
                validate_content_token("tool response identity", &response.tool_call_id, 512)?;
                validate_content_token(
                    "tool occurrence binding",
                    &response.occurrence_binding,
                    512,
                )?;
                validate_content_blocks(&response.content)?;
            }
            Self::Elicitation(response) => response.validate()?,
            Self::Workspace(response) => {
                validate_content_token("workspace receipt identity", &response.change_id, 512)?;
                validate_content_token(
                    "workspace occurrence binding",
                    &response.occurrence_binding,
                    512,
                )?;
                response
                    .evidence
                    .validate()
                    .map_err(|error| ProtocolError::Validation(error.to_string()))?;
            }
        }
        validate_canonical_size("Agent host response", self, MAX_AGENT_VALUE_BYTES)
    }
}

fn validate_response_for_request(
    request: &AgentHostRequest,
    response: &AgentHostResponse,
) -> ProtocolResult<()> {
    if response.kind() != request.kind() {
        return Err(ProtocolError::Validation(
            "host response kind does not match its request".to_owned(),
        ));
    }
    match (request, response) {
        (AgentHostRequest::Model(request), AgentHostResponse::Model(response)) => {
            if response.message.role != MessageRole::Agent {
                return Err(ProtocolError::Validation(
                    "model response must publish an Agent-authored message".to_owned(),
                ));
            }
            for tool in &response.tool_requests {
                if !request.tools.contains(&tool.operation) {
                    return Err(ProtocolError::Validation(format!(
                        "model response requested unadvertised tool operation {}",
                        tool.operation
                    )));
                }
            }
        }
        (AgentHostRequest::Tool(request), AgentHostResponse::Tool(response)) => {
            if request.tool_call_id != response.tool_call_id {
                return Err(ProtocolError::Validation(
                    "tool response identity does not match its request".to_owned(),
                ));
            }
        }
        (AgentHostRequest::Elicitation(request), AgentHostResponse::Elicitation(response)) => {
            validate_elicitation_response(request, response)?;
        }
        (AgentHostRequest::Workspace(request), AgentHostResponse::Workspace(response)) => {
            if request.change().change_id != response.change_id
                || request.change().commit != response.committed
            {
                return Err(ProtocolError::Validation(
                    "workspace receipt identity or decision does not match its request".to_owned(),
                ));
            }
        }
        (AgentHostRequest::Permission(request), AgentHostResponse::Permission(response)) => {
            if !request.options.contains(&response.decision) {
                return Err(ProtocolError::Validation(
                    "permission response selected a decision which was not offered".to_owned(),
                ));
            }
        }
        (AgentHostRequest::Context(request), AgentHostResponse::Context(response)) => {
            if response.source_message_head != request.source_message_head
                || response.source_message_count != request.source_message_count
            {
                return Err(ProtocolError::Validation(
                    "context response changed its pinned source message descriptor".to_owned(),
                ));
            }
        }
        _ => unreachable!("response kind equality closes the request-response union"),
    }
    Ok(())
}

/// Closed meaning of one append-only host recovery observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRecoveryObservationDisposition {
    /// The provider still cannot prove the original world outcome.
    Unknown,
    /// The provider proved that the original dispatch did not apply.
    NotApplied,
}

#[derive(Serialize)]
struct AgentRecoveryObservationIdentity<'a> {
    observation_version: &'static str,
    occurrence_id: &'a str,
    disposition: AgentRecoveryObservationDisposition,
    evidence: &'a [ContentBlock],
}

/// One immutable identity-bound reconciliation observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRecoveryObservation {
    /// Frozen observation identity generation.
    pub observation_version: String,
    /// Content identity of the occurrence, disposition, and complete evidence.
    pub observation_id: String,
    /// Exact occurrence observed by the provider.
    pub occurrence_id: String,
    /// Closed provider conclusion for this observation.
    pub disposition: AgentRecoveryObservationDisposition,
    /// Complete bounded evidence returned by that one observation.
    pub evidence: Vec<ContentBlock>,
}

impl AgentRecoveryObservation {
    fn new(
        occurrence_id: &str,
        disposition: AgentRecoveryObservationDisposition,
        evidence: Vec<ContentBlock>,
    ) -> ProtocolResult<Self> {
        if evidence.is_empty() {
            return Err(ProtocolError::Validation(
                "Agent recovery observation requires non-empty evidence".to_owned(),
            ));
        }
        let observation_id = content_id(
            AGENT_RECOVERY_OBSERVATION_VERSION,
            &AgentRecoveryObservationIdentity {
                observation_version: AGENT_RECOVERY_OBSERVATION_VERSION,
                occurrence_id,
                disposition,
                evidence: &evidence,
            },
        )?;
        let observation = Self {
            observation_version: AGENT_RECOVERY_OBSERVATION_VERSION.to_owned(),
            observation_id,
            occurrence_id: occurrence_id.to_owned(),
            disposition,
            evidence,
        };
        observation.verify()?;
        Ok(observation)
    }

    /// Verify version, owner, evidence, and complete content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the observation is empty, malformed, oversized,
    /// or does not match its complete content-derived identity.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_agent_version(
            "Agent recovery observation",
            &self.observation_version,
            AGENT_RECOVERY_OBSERVATION_VERSION,
        )?;
        validate_identity("Agent recovery occurrence", &self.occurrence_id)?;
        if self.evidence.is_empty() {
            return Err(ProtocolError::Validation(
                "Agent recovery observation requires non-empty evidence".to_owned(),
            ));
        }
        validate_content_blocks(&self.evidence)?;
        let expected = content_id(
            AGENT_RECOVERY_OBSERVATION_VERSION,
            &AgentRecoveryObservationIdentity {
                observation_version: AGENT_RECOVERY_OBSERVATION_VERSION,
                occurrence_id: &self.occurrence_id,
                disposition: self.disposition,
                evidence: &self.evidence,
            },
        )?;
        if self.observation_id != expected {
            return Err(ProtocolError::IdentityMismatch(
                "Agent recovery observation does not match its evidence".to_owned(),
            ));
        }
        validate_canonical_size("Agent recovery observation", self, MAX_AGENT_VALUE_BYTES)
    }
}

/// Durable lifecycle for one host interaction occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHostOccurrenceState {
    /// Binding and request are durable; dispatch has not started.
    Prepared,
    /// Dispatch may have started and requires reconciliation after ambiguity.
    Started,
    /// Typed response is durable.
    Completed,
    /// Dispatch outcome remains ambiguous.
    Unknown,
    /// Provider proved the dispatch did not apply.
    NotApplied,
}

impl AgentHostOccurrenceState {
    /// Stable record-key component.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Unknown => "unknown",
            Self::NotApplied => "not_applied",
        }
    }
}

/// Persisted host interaction with an immutable request and binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHostOccurrence {
    /// Stable caller-supplied occurrence identity.
    pub occurrence_id: String,
    /// Owning Session identity.
    pub session_id: String,
    /// Immutable typed request.
    pub request: AgentHostRequest,
    /// Canonical request digest used for conflict detection.
    pub request_digest: String,
    /// Current durable lifecycle state.
    pub state: AgentHostOccurrenceState,
    /// Retained terminal response, when completed.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub response: Option<AgentHostResponse>,
    /// Complete pinned Agent-host binding selected before dispatch.
    pub occurrence_binding: AgentHostBinding,
    /// Host error summary for an ambiguous outcome.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub failure: Option<String>,
    /// Ordered immutable observations collected during reconciliation.
    pub recovery_observations: Vec<AgentRecoveryObservation>,
}

impl AgentHostOccurrence {
    /// Admit an immutable host request before any provider call begins.
    ///
    /// # Errors
    ///
    /// Returns an error when any identity, request, binding, digest, or bounded
    /// occurrence invariant is invalid.
    pub fn prepare(
        occurrence_id: impl Into<String>,
        session_id: impl Into<String>,
        request: AgentHostRequest,
        occurrence_binding: AgentHostBinding,
    ) -> ProtocolResult<Self> {
        let request_digest = canonical_digest(&request)
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        let occurrence = Self {
            occurrence_id: occurrence_id.into(),
            session_id: session_id.into(),
            request,
            request_digest,
            state: AgentHostOccurrenceState::Prepared,
            response: None,
            occurrence_binding,
            failure: None,
            recovery_observations: Vec::new(),
        };
        occurrence.validate()?;
        Ok(occurrence)
    }

    /// Mark that the host invocation may now have happened.
    ///
    /// # Errors
    ///
    /// Returns an error unless this occurrence may legally advance to Started.
    pub fn start(&self) -> ProtocolResult<Self> {
        self.successor(AgentHostOccurrenceState::Started, None)
    }

    /// Commit a typed response and immutable occurrence binding.
    ///
    /// # Errors
    ///
    /// Returns an error unless the response exactly matches this request and
    /// binding and the lifecycle may legally complete.
    pub fn complete(&self, response: AgentHostResponse) -> ProtocolResult<Self> {
        self.successor(AgentHostOccurrenceState::Completed, Some(response))
    }

    /// Record an ambiguous host result without authorizing redispatch.
    ///
    /// # Errors
    ///
    /// Returns an error when the failure is invalid or the current lifecycle
    /// cannot legally advance to Unknown.
    pub fn mark_unknown(&self, failure: impl Into<String>) -> ProtocolResult<Self> {
        if self.state != AgentHostOccurrenceState::Started {
            return Err(ProtocolError::IllegalTransition(
                "only a Started Agent occurrence can first become Unknown".to_owned(),
            ));
        }
        let next = Self {
            occurrence_id: self.occurrence_id.clone(),
            session_id: self.session_id.clone(),
            request: self.request.clone(),
            request_digest: self.request_digest.clone(),
            state: AgentHostOccurrenceState::Unknown,
            response: None,
            occurrence_binding: self.occurrence_binding.clone(),
            failure: Some(failure.into()),
            recovery_observations: self.recovery_observations.clone(),
        };
        self.validate_successor(&next)?;
        Ok(next)
    }

    /// Record that reconciliation still cannot determine the world outcome.
    ///
    /// # Errors
    ///
    /// Returns an error when the failure or evidence is invalid, or when the
    /// lifecycle cannot legally remain unresolved.
    pub fn mark_unknown_with_evidence(
        &self,
        failure: impl Into<String>,
        evidence: Vec<ContentBlock>,
    ) -> ProtocolResult<Self> {
        let mut next = match self.state {
            AgentHostOccurrenceState::Started => self.mark_unknown(failure)?,
            AgentHostOccurrenceState::Unknown => self.clone(),
            _ => {
                return Err(ProtocolError::IllegalTransition(
                    "only a Started or Unknown Agent occurrence can record unknown evidence"
                        .to_owned(),
                ));
            }
        };
        if evidence.is_empty() {
            return Ok(next);
        }
        let observation = AgentRecoveryObservation::new(
            &self.occurrence_id,
            AgentRecoveryObservationDisposition::Unknown,
            evidence,
        )?;
        if next
            .recovery_observations
            .iter()
            .any(|current| current.observation_id == observation.observation_id)
        {
            return Ok(next);
        }
        next.recovery_observations.push(observation);
        self.validate_successor(&next)?;
        Ok(next)
    }

    /// Settle the occurrence as definitely not applied.
    ///
    /// # Errors
    ///
    /// Returns an error when the evidence is invalid or the lifecycle cannot
    /// legally settle as `NotApplied`.
    pub fn mark_not_applied(&self, evidence: Vec<ContentBlock>) -> ProtocolResult<Self> {
        let observation = AgentRecoveryObservation::new(
            &self.occurrence_id,
            AgentRecoveryObservationDisposition::NotApplied,
            evidence,
        )?;
        if self.state == AgentHostOccurrenceState::NotApplied
            && self
                .recovery_observations
                .last()
                .is_some_and(|current| current == &observation)
        {
            return Ok(self.clone());
        }
        let mut recovery_observations = self.recovery_observations.clone();
        recovery_observations.push(observation);
        let next = Self {
            occurrence_id: self.occurrence_id.clone(),
            session_id: self.session_id.clone(),
            request: self.request.clone(),
            request_digest: self.request_digest.clone(),
            state: AgentHostOccurrenceState::NotApplied,
            response: None,
            occurrence_binding: self.occurrence_binding.clone(),
            failure: None,
            recovery_observations,
        };
        self.validate_successor(&next)?;
        Ok(next)
    }

    /// Whether this occurrence no longer blocks Session recovery.
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            AgentHostOccurrenceState::Completed | AgentHostOccurrenceState::NotApplied
        )
    }

    /// Stable idempotency key for this lifecycle transition.
    ///
    /// # Errors
    ///
    /// Returns an error if the typed transition preimage cannot be canonically
    /// encoded.
    pub fn transition_id(&self) -> ProtocolResult<String> {
        self.validate()?;
        content_id(AGENT_OCCURRENCE_TRANSITION_ID_DOMAIN, self)
            .map_err(|error| ProtocolError::Validation(error.to_string()))
    }

    /// Verify the complete occurrence snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when an identity, request digest, binding, lifecycle
    /// outcome, evidence, or bounded payload is invalid.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.validate_identity_and_artifacts()?;
        match self.state {
            AgentHostOccurrenceState::Prepared | AgentHostOccurrenceState::Started => {
                if self.response.is_some()
                    || self.failure.is_some()
                    || !self.recovery_observations.is_empty()
                {
                    return Err(ProtocolError::Validation(
                        "prepared or started occurrence cannot contain an outcome".to_owned(),
                    ));
                }
            }
            AgentHostOccurrenceState::Completed => {
                let response = self.response.as_ref().ok_or_else(|| {
                    ProtocolError::Validation(
                        "completed occurrence requires a typed response".to_owned(),
                    )
                })?;
                validate_response_for_request(&self.request, response)?;
                if self.occurrence_binding.binding_id() != response.occurrence_binding() {
                    return Err(ProtocolError::Validation(
                        "host occurrence binding does not match its response".to_owned(),
                    ));
                }
                if self.failure.is_some()
                    || self.recovery_observations.iter().any(|observation| {
                        observation.disposition != AgentRecoveryObservationDisposition::Unknown
                    })
                {
                    return Err(ProtocolError::Validation(
                        "completed occurrence can retain only prior unknown observations"
                            .to_owned(),
                    ));
                }
            }
            AgentHostOccurrenceState::Unknown => {
                if self.recovery_observations.len() == MAX_AGENT_RECOVERY_OBSERVATIONS {
                    return Err(ProtocolError::Validation(
                        "Agent occurrence reserves its final recovery-observation slot for terminal not-applied evidence"
                            .to_owned(),
                    ));
                }
                if self.response.is_some() {
                    return Err(ProtocolError::Validation(
                        "unknown occurrence cannot claim a response".to_owned(),
                    ));
                }
                if self.failure.as_deref().is_none_or(str::is_empty) {
                    return Err(ProtocolError::Validation(
                        "unknown occurrence requires failure evidence".to_owned(),
                    ));
                }
                if self.recovery_observations.iter().any(|observation| {
                    observation.disposition != AgentRecoveryObservationDisposition::Unknown
                }) {
                    return Err(ProtocolError::Validation(
                        "unknown occurrence can retain only unknown observations".to_owned(),
                    ));
                }
            }
            AgentHostOccurrenceState::NotApplied => {
                if self.response.is_some()
                    || self.failure.is_some()
                    || self.recovery_observations.is_empty()
                    || self.recovery_observations.last().is_none_or(|observation| {
                        observation.disposition != AgentRecoveryObservationDisposition::NotApplied
                    })
                    || self.recovery_observations[..self.recovery_observations.len() - 1]
                        .iter()
                        .any(|observation| {
                            observation.disposition != AgentRecoveryObservationDisposition::Unknown
                        })
                {
                    return Err(ProtocolError::Validation(
                        "not-applied occurrence requires prior unknown observations followed by one not-applied observation"
                            .to_owned(),
                    ));
                }
            }
        }
        validate_canonical_size("Agent host occurrence", self, MAX_AGENT_VALUE_BYTES)
    }

    fn validate_identity_and_artifacts(&self) -> ProtocolResult<()> {
        validate_identity("Agent host occurrence", &self.occurrence_id)
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        validate_identity("Agent Session", &self.session_id)
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        self.occurrence_binding.verify()?;
        self.request.validate_for_session(&self.session_id)?;
        if let Some(response) = &self.response {
            response.validate_content()?;
        }
        if self.recovery_observations.len() > MAX_AGENT_RECOVERY_OBSERVATIONS {
            return Err(ProtocolError::Validation(format!(
                "Agent occurrence exceeds {MAX_AGENT_RECOVERY_OBSERVATIONS} recovery observations"
            )));
        }
        let mut observation_ids = BTreeSet::new();
        for observation in &self.recovery_observations {
            observation.verify()?;
            if observation.occurrence_id != self.occurrence_id {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent recovery observation changed its occurrence owner".to_owned(),
                ));
            }
            if !observation_ids.insert(&observation.observation_id) {
                return Err(ProtocolError::Validation(
                    "Agent occurrence repeats a recovery observation identity".to_owned(),
                ));
            }
        }
        let expected = canonical_digest(&self.request)
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        if self.request_digest != expected {
            return Err(ProtocolError::Validation(format!(
                "host occurrence {} request digest does not match",
                self.occurrence_id
            )));
        }
        Ok(())
    }

    /// Verify that `next` is a legal immutable transition from this snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when either snapshot is invalid, immutable fields
    /// changed, the transition is a no-op, or the lifecycle edge is illegal.
    pub fn validate_successor(&self, next: &Self) -> ProtocolResult<()> {
        self.validate()?;
        next.validate()?;
        if self == next {
            return Err(ProtocolError::IllegalTransition(
                "host occurrence transition cannot be a no-op".to_owned(),
            ));
        }
        if self.occurrence_id != next.occurrence_id
            || self.session_id != next.session_id
            || self.request != next.request
            || self.request_digest != next.request_digest
            || self.occurrence_binding != next.occurrence_binding
        {
            return Err(ProtocolError::IllegalTransition(
                "host occurrence identity or request changed".to_owned(),
            ));
        }
        if next.recovery_observations.len() < self.recovery_observations.len()
            || next.recovery_observations[..self.recovery_observations.len()]
                != self.recovery_observations
        {
            return Err(ProtocolError::IllegalTransition(
                "Agent recovery observations are append-only".to_owned(),
            ));
        }
        if self.state == AgentHostOccurrenceState::Unknown
            && next.state == AgentHostOccurrenceState::Unknown
            && (self.failure != next.failure
                || next.recovery_observations.len() == self.recovery_observations.len())
        {
            return Err(ProtocolError::IllegalTransition(
                "Unknown Agent occurrence reconciliation must append new evidence without changing the original failure"
                    .to_owned(),
            ));
        }
        if matches!(
            (self.state, next.state),
            (
                AgentHostOccurrenceState::Prepared,
                AgentHostOccurrenceState::Started | AgentHostOccurrenceState::NotApplied
            ) | (
                AgentHostOccurrenceState::Started | AgentHostOccurrenceState::Unknown,
                AgentHostOccurrenceState::Unknown
                    | AgentHostOccurrenceState::Completed
                    | AgentHostOccurrenceState::NotApplied
            )
        ) {
            Ok(())
        } else {
            Err(ProtocolError::IllegalTransition(format!(
                "host occurrence {} cannot transition from {:?} to {:?}",
                self.occurrence_id, self.state, next.state
            )))
        }
    }

    fn successor(
        &self,
        state: AgentHostOccurrenceState,
        response: Option<AgentHostResponse>,
    ) -> ProtocolResult<Self> {
        let next = Self {
            occurrence_id: self.occurrence_id.clone(),
            session_id: self.session_id.clone(),
            request: self.request.clone(),
            request_digest: self.request_digest.clone(),
            state,
            response,
            occurrence_binding: self.occurrence_binding.clone(),
            failure: None,
            recovery_observations: self.recovery_observations.clone(),
        };
        self.validate_successor(&next)?;
        Ok(next)
    }
}

/// Keyed current authority for one host occurrence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOccurrenceCurrent {
    /// Stable Session-local ordinal allocated by the first Prepare transition.
    pub ordinal: u64,
    /// Exact current occurrence snapshot.
    pub occurrence: AgentHostOccurrence,
    /// Exact Agent command which produced this current value.
    pub admitted_by: String,
}

impl AgentOccurrenceCurrent {
    /// Verify the bounded keyed occurrence current.
    ///
    /// # Errors
    ///
    /// Returns an error when the ordinal, occurrence, admitting command, or
    /// bounded current representation is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.ordinal > MAX_EXACT_INTEGER {
            return Err(ProtocolError::Validation(
                "Agent occurrence ordinal exceeds the exact integer range".to_owned(),
            ));
        }
        self.occurrence.validate()?;
        validate_sha256("Agent occurrence command", &self.admitted_by)?;
        validate_canonical_size("Agent occurrence current", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Bounded exact before witness for one occurrence transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOccurrenceSource {
    /// Exact bounded Session metadata before the transition.
    pub session: AgentSessionCurrent,
    /// Exact prior occurrence current, absent only for first Prepare.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current: Option<AgentOccurrenceCurrent>,
}

/// Bounded exact postcondition for one occurrence transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOccurrencePostcondition {
    /// Exact bounded Session metadata after the transition.
    pub session: AgentSessionCurrent,
    /// Exact resulting occurrence current.
    pub current: AgentOccurrenceCurrent,
}

impl AgentOccurrenceSource {
    /// Reduce one occurrence transition without enumerating Session history.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact source does not match the occurrence,
    /// the lifecycle edge is illegal, or the derived bounded state is invalid.
    pub fn reduce(
        &self,
        command_id: &str,
        occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<AgentOccurrencePostcondition> {
        if occurrence.request.is_m1_workspace() {
            return Err(ProtocolError::Validation(
                "M1 workspace occurrences are owned exclusively by AgentWorkspaceCommand"
                    .to_owned(),
            ));
        }
        self.reduce_internal(command_id, occurrence)
    }

    fn reduce_workspace(
        &self,
        command_id: &str,
        occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<AgentOccurrencePostcondition> {
        if !occurrence.request.is_m1_workspace() {
            return Err(ProtocolError::Validation(
                "Agent workspace reducer requires one M1 workspace occurrence".to_owned(),
            ));
        }
        self.reduce_internal(command_id, occurrence)
    }

    fn reduce_internal(
        &self,
        command_id: &str,
        occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<AgentOccurrencePostcondition> {
        let (mut session, ordinal, was_unresolved) =
            prepare_occurrence_transition(self, command_id, occurrence)?;
        apply_unresolved_occurrence_transition(&mut session, ordinal, was_unresolved, occurrence)?;
        session.last_transition = Some(AgentSessionTransitionWitness {
            command_id: command_id.to_owned(),
            kind: AgentSessionTransitionKind::Occurrence,
        });
        session.verify()?;
        let postcondition = AgentOccurrencePostcondition {
            session,
            current: AgentOccurrenceCurrent {
                ordinal,
                occurrence: occurrence.clone(),
                admitted_by: command_id.to_owned(),
            },
        };
        postcondition.verify_for(occurrence)?;
        Ok(postcondition)
    }
}

fn prepare_occurrence_transition(
    source: &AgentOccurrenceSource,
    command_id: &str,
    occurrence: &AgentHostOccurrence,
) -> ProtocolResult<(AgentSessionCurrent, u64, bool)> {
    source.session.verify()?;
    occurrence.validate()?;
    validate_sha256("Agent occurrence command", command_id)?;
    if source.session.state == AgentState::Closed {
        return Err(ProtocolError::IllegalTransition(
            "closed Agent Session cannot admit or advance a host occurrence".to_owned(),
        ));
    }
    if occurrence.session_id != source.session.session_id {
        return Err(ProtocolError::IdentityMismatch(
            "Agent occurrence escaped its Session current".to_owned(),
        ));
    }
    let mut session = source.session.clone();
    let Some(current) = &source.current else {
        if occurrence.state != AgentHostOccurrenceState::Prepared {
            return Err(ProtocolError::IllegalTransition(
                "first Agent occurrence transition must be Prepared".to_owned(),
            ));
        }
        verify_occurrence_admission_session(&session, occurrence)?;
        let ordinal = session.next_occurrence_sequence;
        session.next_occurrence_sequence = session
            .next_occurrence_sequence
            .checked_add(1)
            .ok_or_else(|| {
                ProtocolError::Validation("Agent occurrence sequence is exhausted".to_owned())
            })?;
        return Ok((session, ordinal, false));
    };
    current.verify()?;
    if current.occurrence.session_id != session.session_id
        || current.occurrence.occurrence_id != occurrence.occurrence_id
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent occurrence source changed its keyed owner".to_owned(),
        ));
    }
    current.occurrence.validate_successor(occurrence)?;
    Ok((session, current.ordinal, !current.occurrence.is_terminal()))
}

fn verify_occurrence_admission_session(
    session: &AgentSessionCurrent,
    occurrence: &AgentHostOccurrence,
) -> ProtocolResult<()> {
    let (source_message_head, source_message_count) = match &occurrence.request {
        AgentHostRequest::Context(request) => {
            (&request.source_message_head, request.source_message_count)
        }
        AgentHostRequest::Model(request) => (
            &request.context.source_message_head,
            request.context.source_message_count,
        ),
        AgentHostRequest::Permission(_)
        | AgentHostRequest::Tool(_)
        | AgentHostRequest::Elicitation(_)
        | AgentHostRequest::Workspace(_) => return Ok(()),
    };
    if source_message_head != &session.message_head || source_message_count != session.message_count
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent occurrence changed its admission-time Session message descriptor".to_owned(),
        ));
    }
    Ok(())
}

fn apply_unresolved_occurrence_transition(
    session: &mut AgentSessionCurrent,
    ordinal: u64,
    was_unresolved: bool,
    occurrence: &AgentHostOccurrence,
) -> ProtocolResult<()> {
    match (was_unresolved, !occurrence.is_terminal()) {
        (false, true) => {
            session.unresolved_occurrence_count = session
                .unresolved_occurrence_count
                .checked_add(1)
                .ok_or_else(|| {
                    ProtocolError::Validation(
                        "Agent unresolved occurrence count is exhausted".to_owned(),
                    )
                })?;
            session.unresolved_occurrence_generation = unresolved_generation(
                &session.unresolved_occurrence_generation,
                "put",
                ordinal,
                occurrence,
            )?;
        }
        (true, true) => {
            session.unresolved_occurrence_generation = unresolved_generation(
                &session.unresolved_occurrence_generation,
                "put",
                ordinal,
                occurrence,
            )?;
        }
        (true, false) => {
            session.unresolved_occurrence_count = session
                .unresolved_occurrence_count
                .checked_sub(1)
                .ok_or_else(|| {
                    ProtocolError::IllegalTransition(
                        "Agent unresolved occurrence count underflowed".to_owned(),
                    )
                })?;
            session.unresolved_occurrence_generation = unresolved_generation(
                &session.unresolved_occurrence_generation,
                "remove",
                ordinal,
                occurrence,
            )?;
        }
        (false, false) => {}
    }
    Ok(())
}

impl AgentOccurrencePostcondition {
    /// Verify exact occurrence and Session ownership for one transition.
    ///
    /// # Errors
    ///
    /// Returns an error when either current is invalid or the occurrence does
    /// not exactly match its Session-owned postcondition.
    pub fn verify_for(&self, occurrence: &AgentHostOccurrence) -> ProtocolResult<()> {
        self.session.verify()?;
        self.current.verify()?;
        if &self.current.occurrence != occurrence
            || self.session.session_id != occurrence.session_id
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent occurrence postcondition does not match its command".to_owned(),
            ));
        }
        Ok(())
    }
}

/// One bounded generation-pinned page of unresolved occurrences.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOccurrencePage {
    /// Owning Session identity.
    pub session_id: String,
    /// Exact unresolved-index generation read by this page.
    pub index_generation: String,
    /// Exclusive ordinal cursor, absent for the first page.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub after_ordinal: Option<u64>,
    /// Strictly increasing unresolved occurrence currents.
    pub entries: Vec<AgentOccurrenceCurrent>,
    /// Cursor for the next page, absent when this page reached the end.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub next_after_ordinal: Option<u64>,
}

/// Forward unresolved-occurrence page query pinned to one index generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOccurrencePageQuery {
    /// Stable Session identity.
    pub session_id: String,
    /// Exact unresolved-index generation from Session metadata.
    pub index_generation: String,
    /// Exclusive ordinal cursor, absent for the first page.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub after_ordinal: Option<u64>,
    /// Maximum entries returned, within `1..=MAX_AGENT_PAGE`.
    pub max_entries: u64,
    /// Maximum canonical response bytes, within `1..=MAX_AGENT_PAGE_BYTES`.
    pub max_canonical_bytes: u64,
    /// Exact revision constraint, absent to pin the current head once.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub expected_revision: Option<String>,
}

impl AgentOccurrencePageQuery {
    /// Verify the pinned generation, cursor, and hard page budgets.
    ///
    /// # Errors
    ///
    /// Returns an error when the Session, generation, cursor, revision, or page
    /// budget is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        validate_sha256(
            "Agent unresolved occurrence generation",
            &self.index_generation,
        )?;
        if self
            .after_ordinal
            .is_some_and(|value| value > MAX_EXACT_INTEGER)
            || self.max_entries == 0
            || self.max_entries > MAX_AGENT_PAGE as u64
            || self.max_canonical_bytes == 0
            || self.max_canonical_bytes > MAX_AGENT_PAGE_BYTES as u64
        {
            return Err(ProtocolError::Validation(
                "Agent occurrence page query exceeds its cursor or page budget".to_owned(),
            ));
        }
        verify_optional_revision(self.expected_revision.as_ref())
    }
}

impl AgentOccurrencePage {
    /// Verify page bounds, order, state, owner, and cursor shape.
    ///
    /// # Errors
    ///
    /// Returns an error when the page exceeds its budgets or contains invalid,
    /// terminal, out-of-order, cross-Session, or cursor-mismatched entries.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        validate_sha256(
            "Agent unresolved occurrence generation",
            &self.index_generation,
        )?;
        if self.entries.len() > MAX_AGENT_PAGE {
            return Err(ProtocolError::Validation(format!(
                "Agent occurrence page exceeds {MAX_AGENT_PAGE} entries"
            )));
        }
        let mut previous = self.after_ordinal;
        for entry in &self.entries {
            entry.verify()?;
            if entry.occurrence.session_id != self.session_id
                || entry.occurrence.is_terminal()
                || previous.is_some_and(|value| entry.ordinal <= value)
            {
                return Err(ProtocolError::Validation(
                    "Agent unresolved occurrence page is not strictly ordered".to_owned(),
                ));
            }
            previous = Some(entry.ordinal);
        }
        if self.next_after_ordinal.is_some() && self.next_after_ordinal != previous {
            return Err(ProtocolError::Validation(
                "Agent unresolved occurrence cursor does not match its last entry".to_owned(),
            ));
        }
        if self.entries.is_empty() && self.next_after_ordinal.is_some() {
            return Err(ProtocolError::Validation(
                "empty Agent occurrence page cannot carry a next cursor".to_owned(),
            ));
        }
        validate_canonical_size("Agent occurrence page", self, MAX_AGENT_PAGE_BYTES)
    }
}

/// Revision-pinned bounded ordered-message page read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMessagePageRead {
    /// Exact `StateRoot` revision read by the query.
    pub revision: String,
    /// Head- and cursor-pinned bounded page.
    pub page: AgentMessagePage,
}

impl AgentMessagePageRead {
    /// Verify revision, query identity, page budget, order, and cursor coupling.
    ///
    /// # Errors
    ///
    /// Returns an error when the query, revision, page, owner, head, cursor, or
    /// requested response budget does not exactly match.
    pub fn verify_for(&self, query: &AgentMessagePageQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_read_revision(&self.revision, query.expected_revision.as_ref())?;
        self.page.verify()?;
        let max_entries = verified_page_usize(query.max_entries);
        if self.page.session_id != query.session_id
            || self.page.expected_message_head != query.expected_message_head
            || self.page.source_message_count != query.source_message_count
            || self.page.end_exclusive != query.end_exclusive
            || self.page.entries.len() > max_entries
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent message page read does not match its pinned query".to_owned(),
            ));
        }
        if message_page_entry_canonical_bytes(&self.page.entries)?
            > verified_page_usize(query.max_message_canonical_bytes)
        {
            return Err(ProtocolError::Validation(
                "Agent message page read exceeds its message-current byte budget".to_owned(),
            ));
        }
        validate_canonical_size(
            "Agent message page read",
            self,
            verified_page_usize(query.max_canonical_bytes),
        )
    }
}

/// Revision-pinned exact tool current read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentToolRead {
    /// Exact `StateRoot` revision read by the query.
    pub revision: String,
    /// Current tool projection, absent when the exact key does not exist.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current: Option<AgentToolCurrent>,
}

impl AgentToolRead {
    /// Verify revision pinning and exact Session/tool ownership for one query.
    ///
    /// # Errors
    ///
    /// Returns an error when the query or revision is invalid, or when the
    /// returned tool current does not match the exact queried key.
    pub fn verify_for(&self, query: &AgentToolQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_read_revision(&self.revision, query.expected_revision.as_ref())?;
        if let Some(current) = &self.current {
            current.verify()?;
            if current.session_id != query.session_id
                || current.tool.tool_call_id != query.tool_call_id
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent tool read changed its exact key".to_owned(),
                ));
            }
        }
        validate_canonical_size("Agent tool read", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Revision-pinned exact elicitation current read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentElicitationRead {
    /// Exact `StateRoot` revision read by the query.
    pub revision: String,
    /// Current elicitation projection, absent when the exact key does not exist.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current: Option<AgentElicitationCurrent>,
}

impl AgentElicitationRead {
    /// Verify revision pinning and exact Session/request ownership for one query.
    ///
    /// # Errors
    ///
    /// Returns an error when the query or revision is invalid, or when the
    /// returned elicitation current does not match the exact queried key.
    pub fn verify_for(&self, query: &AgentElicitationQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_read_revision(&self.revision, query.expected_revision.as_ref())?;
        if let Some(current) = &self.current {
            current.verify()?;
            if current.session_id != query.session_id
                || current.elicitation.request.request_id != query.request_id
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent elicitation read changed its exact key".to_owned(),
                ));
            }
        }
        validate_canonical_size("Agent elicitation read", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Revision-pinned exact occurrence current read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOccurrenceRead {
    /// Exact `StateRoot` revision read by the query.
    pub revision: String,
    /// Current occurrence projection, absent when the exact key does not exist.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current: Option<AgentOccurrenceCurrent>,
}

impl AgentOccurrenceRead {
    /// Verify revision pinning and exact Session/occurrence ownership for one query.
    ///
    /// # Errors
    ///
    /// Returns an error when the query or revision is invalid, or when the
    /// returned occurrence current does not match the exact queried key.
    pub fn verify_for(&self, query: &AgentOccurrenceQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_read_revision(&self.revision, query.expected_revision.as_ref())?;
        if let Some(current) = &self.current {
            current.verify()?;
            if current.occurrence.session_id != query.session_id
                || current.occurrence.occurrence_id != query.occurrence_id
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent occurrence read changed its exact key".to_owned(),
                ));
            }
        }
        validate_canonical_size("Agent occurrence read", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Revision-pinned bounded unresolved-occurrence page read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentOccurrencePageRead {
    /// Exact `StateRoot` revision read by the query.
    pub revision: String,
    /// Generation- and cursor-pinned bounded page.
    pub page: AgentOccurrencePage,
}

impl AgentOccurrencePageRead {
    /// Verify revision, query identity, page budget, order, and cursor coupling.
    ///
    /// # Errors
    ///
    /// Returns an error when the query, revision, page, generation, cursor, or
    /// requested response budget does not exactly match.
    pub fn verify_for(&self, query: &AgentOccurrencePageQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_read_revision(&self.revision, query.expected_revision.as_ref())?;
        self.page.verify()?;
        let max_entries = verified_page_usize(query.max_entries);
        if self.page.session_id != query.session_id
            || self.page.index_generation != query.index_generation
            || self.page.after_ordinal != query.after_ordinal
            || self.page.entries.len() > max_entries
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent occurrence page read does not match its pinned query".to_owned(),
            ));
        }
        validate_canonical_size(
            "Agent occurrence page read",
            self,
            verified_page_usize(query.max_canonical_bytes),
        )
    }
}

fn verified_page_usize(value: u64) -> usize {
    usize::try_from(value).expect("verified Agent page bound fits every supported usize")
}

fn unresolved_generation(
    previous: &str,
    operation: &str,
    ordinal: u64,
    occurrence: &AgentHostOccurrence,
) -> ProtocolResult<String> {
    content_id(
        AGENT_UNRESOLVED_OCCURRENCE_GENERATION_DOMAIN,
        &(
            previous,
            operation,
            ordinal,
            occurrence.occurrence_id.as_str(),
            occurrence.state,
            occurrence.transition_id()?,
        ),
    )
    .map_err(Into::into)
}

fn open_stream_generation(
    previous: &str,
    operation: &str,
    stream_id: &str,
    command_id: &str,
) -> ProtocolResult<String> {
    content_id(
        AGENT_OPEN_STREAM_GENERATION_DOMAIN,
        &(previous, operation, stream_id, command_id),
    )
    .map_err(Into::into)
}

fn close_open_stream(
    session: &AgentSessionCurrent,
    stream_id: &str,
    command_id: &str,
) -> ProtocolResult<AgentSessionCurrent> {
    let mut session = session.clone();
    session.open_stream_count = session.open_stream_count.checked_sub(1).ok_or_else(|| {
        ProtocolError::IllegalTransition("Agent open stream count underflowed".to_owned())
    })?;
    session.open_stream_generation = open_stream_generation(
        &session.open_stream_generation,
        "remove",
        stream_id,
        command_id,
    )?;
    session.last_transition = Some(AgentSessionTransitionWitness {
        command_id: command_id.to_owned(),
        kind: AgentSessionTransitionKind::Stream,
    });
    session.verify()?;
    Ok(session)
}

/// Maximum canonical JSON bytes retained in one stream chunk.
pub const AGENT_STREAM_CHUNK_LIMIT: usize = MAX_AGENT_VALUE_BYTES;
/// Hard maximum number of immutable chunks retained by one staged stream.
pub const MAX_AGENT_STREAM_CHUNKS: usize = 64;
/// Maximum cumulative canonical chunk bytes admitted before finalization.
pub const AGENT_STREAM_STAGING_BYTES_LIMIT: usize = MAX_AGENT_VALUE_BYTES;

/// Final Session object produced by one stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentStreamTarget {
    /// One finalized Session message.
    Message {
        /// Stable message identity shared by all chunks.
        message_id: String,
        /// Final message author.
        role: MessageRole,
    },
    /// Final output of one already in-progress tool call.
    Tool {
        /// Stable tool-call identity.
        tool_call_id: String,
    },
}

impl AgentStreamTarget {
    /// Verify the target's self-contained identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the target identity is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        match self {
            Self::Message { message_id, .. } => {
                validate_identity("Agent stream message", message_id)?;
            }
            Self::Tool { tool_call_id } => {
                validate_identity("Agent stream tool", tool_call_id)?;
            }
        }
        Ok(())
    }
}

/// Session-local identity governed by one Agent target claim.
///
/// Message role is intentionally absent: the physical claim key is exactly
/// `(session_id, target kind, local identity)`, so two roles cannot create
/// parallel authorities for the same immutable Message slot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentTargetClaimTarget {
    /// One immutable Message slot.
    Message {
        /// Stable message identity.
        message_id: String,
    },
    /// One Tool lifecycle slot.
    Tool {
        /// Stable tool-call identity.
        tool_call_id: String,
    },
}

impl AgentTargetClaimTarget {
    /// Derive the role-free claim target from one stream target.
    #[must_use]
    pub fn from_stream_target(target: &AgentStreamTarget) -> Self {
        match target {
            AgentStreamTarget::Message { message_id, .. } => Self::Message {
                message_id: message_id.clone(),
            },
            AgentStreamTarget::Tool { tool_call_id } => Self::Tool {
                tool_call_id: tool_call_id.clone(),
            },
        }
    }

    /// Verify the exact local identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the local Message or Tool identity is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        match self {
            Self::Message { message_id } => validate_identity("Agent target Message", message_id),
            Self::Tool { tool_call_id } => validate_identity("Agent target Tool", tool_call_id),
        }
        .map_err(Into::into)
    }
}

/// Stable `StateRoot` key for one exact Session-local target claim.
///
/// # Errors
///
/// Returns an error when the Session or target identity is invalid.
pub fn agent_target_claim_key(
    session_id: &str,
    target: &AgentTargetClaimTarget,
) -> ProtocolResult<String> {
    validate_identity("Agent Session", session_id)?;
    target.verify()?;
    content_id(AGENT_TARGET_CLAIM_KEY_DOMAIN, &(session_id, target)).map_err(Into::into)
}

/// Stable `StateRoot` key for one immutable target-claim generation slot.
///
/// # Errors
///
/// Returns an error when the Session, target, or generation is invalid.
pub fn agent_target_claim_generation_key(
    session_id: &str,
    target: &AgentTargetClaimTarget,
    generation: u64,
) -> ProtocolResult<String> {
    validate_identity("Agent Session", session_id)?;
    target.verify()?;
    if generation == 0 || generation > MAX_EXACT_INTEGER {
        return Err(ProtocolError::Validation(
            "Agent target-claim generation is outside the exact integer range".to_owned(),
        ));
    }
    content_id(
        AGENT_TARGET_CLAIM_GENERATION_KEY_DOMAIN,
        &(session_id, target, generation),
    )
    .map_err(Into::into)
}

/// Closed lifecycle phase of one exact Agent target claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentTargetClaimPhase {
    /// An external stream exclusively owns the target before provider I/O.
    Reserved {
        /// Owning stream identity.
        stream_id: String,
        /// Exact publication reservation identity.
        reservation_id: String,
    },
    /// The target has been terminally materialized and cannot be reused.
    Materialized,
    /// Durable `NotApplied` Abort released the former reservation.
    Released {
        /// Former owning stream identity.
        stream_id: String,
        /// Exact released publication reservation identity.
        reservation_id: String,
    },
}

impl AgentTargetClaimPhase {
    fn verify(&self) -> ProtocolResult<()> {
        match self {
            Self::Reserved {
                stream_id,
                reservation_id,
            }
            | Self::Released {
                stream_id,
                reservation_id,
            } => {
                validate_identity("Agent target-claim stream", stream_id)?;
                validate_sha256("Agent target-claim reservation", reservation_id)
            }
            Self::Materialized => Ok(()),
        }
    }
}

#[derive(Serialize)]
struct AgentTargetClaimIdentity<'a> {
    current_version: &'a str,
    session_id: &'a str,
    target: &'a AgentTargetClaimTarget,
    generation: u64,
    predecessor_claim_id: Option<&'a str>,
    predecessor_admitted_by: Option<&'a str>,
    phase: &'a AgentTargetClaimPhase,
    admitted_by: &'a str,
}

/// Exact generation-bearing authority for one Agent target slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentTargetClaimCurrent {
    /// Current wire generation.
    pub current_version: String,
    /// Content identity of this exact generation and phase.
    pub claim_id: String,
    /// Owning Session identity.
    pub session_id: String,
    /// Role-free target identity.
    pub target: AgentTargetClaimTarget,
    /// Monotonic one-based generation preventing release/reuse ABA.
    pub generation: u64,
    /// Exact immediate predecessor claim identity, absent only at generation one.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub predecessor_claim_id: Option<String>,
    /// Command identity which admitted the immediate predecessor, absent at genesis.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub predecessor_admitted_by: Option<String>,
    /// Closed reservation/materialization lifecycle.
    pub phase: AgentTargetClaimPhase,
    /// Exact Agent command admitting this generation.
    pub admitted_by: String,
}

impl AgentTargetClaimCurrent {
    fn new(
        session_id: &str,
        target: AgentTargetClaimTarget,
        generation: u64,
        predecessor_claim_id: Option<&str>,
        predecessor_admitted_by: Option<&str>,
        phase: AgentTargetClaimPhase,
        admitted_by: &str,
    ) -> ProtocolResult<Self> {
        let claim_id = content_id(
            AGENT_TARGET_CLAIM_ID_DOMAIN,
            &AgentTargetClaimIdentity {
                current_version: AGENT_TARGET_CLAIM_CURRENT_VERSION,
                session_id,
                target: &target,
                generation,
                predecessor_claim_id,
                predecessor_admitted_by,
                phase: &phase,
                admitted_by,
            },
        )?;
        let current = Self {
            current_version: AGENT_TARGET_CLAIM_CURRENT_VERSION.to_owned(),
            claim_id,
            session_id: session_id.to_owned(),
            target,
            generation,
            predecessor_claim_id: predecessor_claim_id.map(str::to_owned),
            predecessor_admitted_by: predecessor_admitted_by.map(str::to_owned),
            phase,
            admitted_by: admitted_by.to_owned(),
        };
        current.verify()?;
        Ok(current)
    }

    /// Verify the complete content-derived claim generation.
    ///
    /// # Errors
    ///
    /// Returns an error when version, owner, target, generation, phase,
    /// admitting command, identity, or bounded representation changed.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_agent_version(
            "Agent target claim current",
            &self.current_version,
            AGENT_TARGET_CLAIM_CURRENT_VERSION,
        )?;
        validate_identity("Agent Session", &self.session_id)?;
        self.target.verify()?;
        if self.generation == 0 || self.generation > MAX_EXACT_INTEGER {
            return Err(ProtocolError::Validation(
                "Agent target-claim generation is outside the exact integer range".to_owned(),
            ));
        }
        if (self.generation == 1)
            != (self.predecessor_claim_id.is_none() && self.predecessor_admitted_by.is_none())
            || self.predecessor_claim_id.is_some() != self.predecessor_admitted_by.is_some()
        {
            return Err(ProtocolError::Validation(
                "Agent target-claim predecessor does not match its generation".to_owned(),
            ));
        }
        if let Some(predecessor_claim_id) = &self.predecessor_claim_id {
            validate_sha256("Agent target-claim predecessor", predecessor_claim_id)?;
        }
        if let Some(predecessor_admitted_by) = &self.predecessor_admitted_by {
            validate_sha256(
                "Agent target-claim predecessor command",
                predecessor_admitted_by,
            )?;
        }
        self.phase.verify()?;
        validate_sha256("Agent target-claim command", &self.admitted_by)?;
        let expected = content_id(
            AGENT_TARGET_CLAIM_ID_DOMAIN,
            &AgentTargetClaimIdentity {
                current_version: &self.current_version,
                session_id: &self.session_id,
                target: &self.target,
                generation: self.generation,
                predecessor_claim_id: self.predecessor_claim_id.as_deref(),
                predecessor_admitted_by: self.predecessor_admitted_by.as_deref(),
                phase: &self.phase,
                admitted_by: &self.admitted_by,
            },
        )?;
        if self.claim_id != expected {
            return Err(ProtocolError::IdentityMismatch(
                "Agent target claim does not match its exact generation".to_owned(),
            ));
        }
        validate_canonical_size("Agent target claim current", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Immutable membership record for one exact target-claim generation.
///
/// The current claim remains the sole mutable authority for a target. This
/// record is the append-only generation index used to prove historical
/// membership without walking predecessor receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentTargetClaimGenerationRecord {
    /// Frozen record generation.
    pub record_version: String,
    /// Owning Session identity.
    pub session_id: String,
    /// Role-free target identity.
    pub target: AgentTargetClaimTarget,
    /// Exact one-based generation slot.
    pub generation: u64,
    /// Content identity of the exact claim stored at this slot.
    pub claim_id: String,
    /// Exact Agent command which admitted this claim generation.
    pub admitted_by: String,
}

impl AgentTargetClaimGenerationRecord {
    /// Derive the immutable generation slot from one verified claim current.
    ///
    /// # Errors
    ///
    /// Returns an error when the claim current or derived record is invalid.
    pub fn from_current(current: &AgentTargetClaimCurrent) -> ProtocolResult<Self> {
        current.verify()?;
        let record = Self {
            record_version: AGENT_TARGET_CLAIM_GENERATION_RECORD_VERSION.to_owned(),
            session_id: current.session_id.clone(),
            target: current.target.clone(),
            generation: current.generation,
            claim_id: current.claim_id.clone(),
            admitted_by: current.admitted_by.clone(),
        };
        record.verify()?;
        Ok(record)
    }

    /// Verify this exact immutable generation membership record.
    ///
    /// # Errors
    ///
    /// Returns an error when its version, owner, target, generation, claim,
    /// command, key material, or bounded encoding is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_agent_version(
            "Agent target claim generation record",
            &self.record_version,
            AGENT_TARGET_CLAIM_GENERATION_RECORD_VERSION,
        )?;
        agent_target_claim_generation_key(&self.session_id, &self.target, self.generation)?;
        validate_sha256("Agent target-claim generation claim", &self.claim_id)?;
        validate_sha256("Agent target-claim generation command", &self.admitted_by)?;
        validate_canonical_size(
            "Agent target claim generation record",
            self,
            MAX_AGENT_CURRENT_BYTES,
        )
    }

    /// Verify that this slot records one exact claim current.
    ///
    /// # Errors
    ///
    /// Returns an error when the record and claim differ.
    pub fn verify_for(&self, current: &AgentTargetClaimCurrent) -> ProtocolResult<()> {
        self.verify()?;
        current.verify()?;
        if self.session_id != current.session_id
            || self.target != current.target
            || self.generation != current.generation
            || self.claim_id != current.claim_id
            || self.admitted_by != current.admitted_by
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent target-claim generation record changed its exact claim".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Exact source membership or non-membership used by one target transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentTargetClaimSource {
    /// Role-free exact target identity.
    pub target: AgentTargetClaimTarget,
    /// Current generation, absent before first ownership.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current: Option<AgentTargetClaimCurrent>,
}

impl AgentTargetClaimSource {
    fn verify_for(&self, session_id: &str) -> ProtocolResult<()> {
        self.target.verify()?;
        if let Some(current) = &self.current {
            current.verify()?;
            if current.session_id != session_id || current.target != self.target {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent target-claim source changed its exact key".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// Closed exact before/after mutation for the independent target-claim family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentTargetClaimTransition {
    /// Exact source generation or proved absence.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub source: Option<AgentTargetClaimCurrent>,
    /// Exact successor generation.
    pub current: AgentTargetClaimCurrent,
}

impl AgentTargetClaimTransition {
    fn new(
        session_id: &str,
        target: AgentTargetClaimTarget,
        source: Option<&AgentTargetClaimCurrent>,
        phase: AgentTargetClaimPhase,
        admitted_by: &str,
    ) -> ProtocolResult<Self> {
        if let Some(source) = source {
            source.verify()?;
            if source.session_id != session_id || source.target != target {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent target-claim transition changed its exact key".to_owned(),
                ));
            }
        }
        validate_target_claim_phase_transition(source, &phase, admitted_by)?;
        let generation = source.map_or(Ok(1), |source| {
            source.generation.checked_add(1).ok_or_else(|| {
                ProtocolError::Validation("Agent target-claim generation is exhausted".to_owned())
            })
        })?;
        let transition = Self {
            source: source.cloned(),
            current: AgentTargetClaimCurrent::new(
                session_id,
                target,
                generation,
                source.map(|current| current.claim_id.as_str()),
                source.map(|current| current.admitted_by.as_str()),
                phase,
                admitted_by,
            )?,
        };
        transition.verify()?;
        Ok(transition)
    }

    /// Verify the exact generation successor and closed phase transition.
    ///
    /// # Errors
    ///
    /// Returns an error when the source, key, generation, phase, command, or
    /// derived successor changed.
    pub fn verify(&self) -> ProtocolResult<()> {
        self.current.verify()?;
        if let Some(source) = &self.source {
            source.verify()?;
            if source.session_id != self.current.session_id
                || source.target != self.current.target
                || source.generation.checked_add(1) != Some(self.current.generation)
                || self.current.predecessor_claim_id.as_deref() != Some(source.claim_id.as_str())
                || self.current.predecessor_admitted_by.as_deref()
                    != Some(source.admitted_by.as_str())
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent target-claim transition changed its key or generation".to_owned(),
                ));
            }
        } else if self.current.generation != 1
            || self.current.predecessor_claim_id.is_some()
            || self.current.predecessor_admitted_by.is_some()
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent target-claim genesis must use generation one".to_owned(),
            ));
        }
        validate_target_claim_phase_transition(
            self.source.as_ref(),
            &self.current.phase,
            &self.current.admitted_by,
        )
    }
}

fn validate_target_claim_phase_transition(
    source: Option<&AgentTargetClaimCurrent>,
    next: &AgentTargetClaimPhase,
    admitted_by: &str,
) -> ProtocolResult<()> {
    validate_sha256("Agent target-claim command", admitted_by)?;
    next.verify()?;
    match (source.map(|current| &current.phase), next) {
        (
            None | Some(AgentTargetClaimPhase::Released { .. }),
            AgentTargetClaimPhase::Reserved { .. } | AgentTargetClaimPhase::Materialized,
        ) => Ok(()),
        (
            Some(AgentTargetClaimPhase::Reserved {
                stream_id: source_stream,
                reservation_id: source_reservation,
            }),
            AgentTargetClaimPhase::Released {
                stream_id,
                reservation_id,
            },
        ) if source_stream == stream_id && source_reservation == reservation_id => Ok(()),
        (Some(AgentTargetClaimPhase::Reserved { .. }), AgentTargetClaimPhase::Materialized)
            if source.is_some_and(|current| current.admitted_by == admitted_by) =>
        {
            Ok(())
        }
        _ => Err(ProtocolError::IllegalTransition(
            "Agent target claim does not admit the requested phase successor".to_owned(),
        )),
    }
}

/// Immutable expected content of one external stream publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamPublicationContent {
    /// Exact media type of the published Resource.
    pub media_type: String,
    /// Exact content digest of the published bytes.
    pub digest: String,
    /// Exact byte length of the published bytes.
    pub size: u64,
}

impl AgentStreamPublicationContent {
    /// Verify the complete immutable content descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error when the media type, digest, size, or bounded encoding
    /// is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        crate::resource::validate_resource_media_type(&self.media_type)
            .map_err(resource_protocol_error)?;
        validate_sha256("Agent stream publication content", &self.digest)?;
        if self.size > MAX_EXACT_INTEGER {
            return Err(ProtocolError::Validation(
                "Agent stream publication size exceeds the exact integer range".to_owned(),
            ));
        }
        validate_canonical_size(
            "Agent stream publication content",
            self,
            MAX_AGENT_VALUE_BYTES,
        )
    }
}

fn external_stream_resource_handle(
    content: &AgentStreamPublicationContent,
) -> ProtocolResult<ResourceHandle> {
    content.verify()?;
    crate::resource::ResourceCandidate {
        resource_version: crate::resource::RESOURCE_VERSION.to_owned(),
        shape: crate::resource::ResourceShape::Object,
        media_type: content.media_type.clone(),
        inline: None,
        integrity: crate::resource::ResourceIntegrity::Content {
            digest: content.digest.clone(),
            size: content.size,
        },
        manifest: None,
        annotations: BTreeMap::default(),
    }
    .seal()
    .map_err(resource_protocol_error)
}

/// Immutable output authority selected when one stream is opened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "delivery", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentStreamDelivery {
    /// Content is accumulated as bounded immutable Agent chunk entries.
    Staged,
    /// Final content is obtained from one exact non-Serde Resource authority.
    ExternalResource {
        /// Immutable resolver/provider implementation binding.
        resolver_binding: String,
        /// Exact content which that binding must idempotently publish and read back.
        content: AgentStreamPublicationContent,
    },
}

impl AgentStreamDelivery {
    /// Verify the immutable delivery authority.
    ///
    /// # Errors
    ///
    /// Returns an error when an external delivery has an invalid resolver binding.
    pub fn verify(&self) -> ProtocolResult<()> {
        match self {
            Self::Staged => Ok(()),
            Self::ExternalResource {
                resolver_binding,
                content,
            } => {
                validate_identity("Agent stream Resource resolver binding", resolver_binding)?;
                content.verify()
            }
        }
    }
}

/// One ordered, non-final stream chunk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamChunk {
    /// Zero-based contiguous sequence.
    pub sequence: u64,
    /// Protocol-neutral content fragments.
    pub content: Vec<ContentBlock>,
}

impl AgentStreamChunk {
    /// Verify bounds and finalized content closure without reading stream state.
    ///
    /// # Errors
    ///
    /// Returns an error when the sequence, content, inline Resource closure, or
    /// canonical byte size violates the bounded chunk contract.
    pub fn verify(&self) -> ProtocolResult<()> {
        if self.sequence > MAX_EXACT_INTEGER {
            return Err(ProtocolError::Validation(
                "Agent stream chunk sequence exceeds the exact integer range".to_owned(),
            ));
        }
        if self.content.is_empty() {
            return Err(ProtocolError::Validation(
                "Agent stream chunk content must not be empty".to_owned(),
            ));
        }
        validate_count(
            "Agent stream chunk content",
            self.content.len(),
            MAX_AGENT_VALUE_ENTRIES,
        )?;
        if cymule_core::canonical_bytes(&self.content)?.len() > AGENT_STREAM_CHUNK_LIMIT {
            return Err(ProtocolError::Validation(format!(
                "Agent stream chunk exceeds {AGENT_STREAM_CHUNK_LIMIT} canonical bytes"
            )));
        }
        for block in &self.content {
            block.validate()?;
            if matches!(block, ContentBlock::Text { text } if text.is_empty()) {
                return Err(ProtocolError::Validation(
                    "Agent stream text chunk must not be empty".to_owned(),
                ));
            }
            if let ContentBlock::ResourceHandle { resource } = block
                && (resource.inline.is_none()
                    || !matches!(
                        resource.integrity,
                        crate::resource::ResourceIntegrity::Inline
                    ))
            {
                return Err(ProtocolError::Validation(
                    "Agent stream chunks require inline Resource Handles".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// Stream lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStreamState {
    /// Chunks may still be appended.
    Open,
    /// One final Session update is durable.
    Finalized,
    /// No final Session update will be published.
    Aborted,
}

/// Bounded keyed current for one staged stream; chunk payloads live in ordinal entries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamCurrent {
    /// Stable stream identity.
    pub stream_id: String,
    /// Owning Session identity.
    pub session_id: String,
    /// Immutable final target.
    pub target: AgentStreamTarget,
    /// Immutable source of the final content.
    pub delivery: AgentStreamDelivery,
    /// Durable provider-dispatch reservation for one external Finalize.
    ///
    /// This is present only while an external stream remains open between its
    /// pre-publication retention CAS and terminal finalization.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub publication_reservation: Option<Box<AgentStreamPublicationReservation>>,
    /// Current lifecycle state.
    pub state: AgentStreamState,
    /// Next zero-based chunk sequence.
    pub next_chunk_sequence: u64,
    /// Content head of the staged chunk order.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub chunk_head: Option<String>,
    /// Cumulative canonical bytes of staged chunk entries.
    pub staged_bytes: u64,
    /// Cumulative content blocks retained by the staged chunks and their final output.
    pub staged_content_blocks: u64,
    /// Exact canonical bytes of the prospective final update, including its wrapper.
    pub final_update_bytes: u64,
    /// Final Session update, present only after finalization.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub final_update: Option<AgentUpdate>,
    /// Raw canonical SHA-256 digest of the exact finalized update content.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub content_digest: Option<String>,
    /// Stable abort reason, present only after abort.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub abort_reason: Option<String>,
    /// Exact Agent command which produced this current value.
    pub admitted_by: String,
}

impl AgentStreamCurrent {
    /// Verify the complete bounded stream current without reading chunk payloads.
    ///
    /// # Errors
    ///
    /// Returns an error when any identity, delivery, counter, head, lifecycle
    /// outcome, final update, or bounded representation is inconsistent.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent stream", &self.stream_id)?;
        validate_identity("Agent Session", &self.session_id)?;
        self.target.verify()?;
        self.delivery.verify()?;
        validate_sha256("Agent stream command", &self.admitted_by)?;
        if self.next_chunk_sequence > MAX_AGENT_STREAM_CHUNKS as u64
            || self.staged_bytes > AGENT_STREAM_STAGING_BYTES_LIMIT as u64
            || self.staged_content_blocks > MAX_AGENT_VALUE_ENTRIES as u64
            || self.final_update_bytes == 0
            || self.final_update_bytes > MAX_AGENT_VALUE_BYTES as u64
        {
            return Err(ProtocolError::Validation(
                "Agent stream current exceeds its exact bounded staging range".to_owned(),
            ));
        }
        if (self.next_chunk_sequence == 0) != self.chunk_head.is_none() {
            return Err(ProtocolError::Validation(
                "Agent stream chunk count does not match its staged head".to_owned(),
            ));
        }
        if self.staged_content_blocks < self.next_chunk_sequence
            || (self.next_chunk_sequence == 0 && self.staged_content_blocks != 0)
        {
            return Err(ProtocolError::Validation(
                "Agent stream content count does not match its staged chunks".to_owned(),
            ));
        }
        self.verify_delivery_state()?;
        if let Some(head) = &self.chunk_head {
            validate_sha256("Agent stream chunk head", head)?;
        }
        self.verify_lifecycle_state()?;
        validate_canonical_size("Agent stream current", self, MAX_AGENT_CURRENT_BYTES)
    }

    fn verify_lifecycle_state(&self) -> ProtocolResult<()> {
        match self.state {
            AgentStreamState::Open => self.verify_open_state()?,
            AgentStreamState::Finalized => self.verify_finalized_state()?,
            AgentStreamState::Aborted => self.verify_aborted_state()?,
        }
        Ok(())
    }

    fn verify_open_state(&self) -> ProtocolResult<()> {
        if self.final_update.is_some()
            || self.content_digest.is_some()
            || self.abort_reason.is_some()
        {
            return Err(ProtocolError::Validation(
                "open Agent stream cannot retain a terminal outcome".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_finalized_state(&self) -> ProtocolResult<()> {
        if self.publication_reservation.is_some() {
            return Err(ProtocolError::Validation(
                "finalized Agent stream cannot retain a publication reservation".to_owned(),
            ));
        }
        let update = self.final_update.as_ref().ok_or_else(|| {
            ProtocolError::Validation(
                "finalized Agent stream current requires its Session update".to_owned(),
            )
        })?;
        update.validate_content()?;
        if self.final_update_bytes != cymule_core::canonical_bytes(update)?.len() as u64 {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream final update byte count does not match its admitted capacity"
                    .to_owned(),
            ));
        }
        if update.update_id() != agent_stream_final_update_id(&self.session_id, &self.stream_id)?
            || self.abort_reason.is_some()
        {
            return Err(ProtocolError::Validation(
                "finalized Agent stream current has inconsistent terminal fields".to_owned(),
            ));
        }
        let content_digest = self.content_digest.as_deref().ok_or_else(|| {
            ProtocolError::Validation(
                "finalized Agent stream current requires its content digest".to_owned(),
            )
        })?;
        validate_canonical_digest("Agent stream finalized content digest", content_digest)?;
        if content_digest != agent_stream_final_update_content_digest(&self.target, update)? {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream finalized content digest does not match its final update".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_aborted_state(&self) -> ProtocolResult<()> {
        if self.publication_reservation.is_some() {
            return Err(ProtocolError::Validation(
                "aborted Agent stream cannot retain a publication reservation".to_owned(),
            ));
        }
        if self.final_update.is_some() || self.content_digest.is_some() {
            return Err(ProtocolError::Validation(
                "aborted Agent stream cannot retain finalized output".to_owned(),
            ));
        }
        validate_identity(
            "Agent stream abort reason",
            self.abort_reason.as_deref().ok_or_else(|| {
                ProtocolError::Validation("aborted Agent stream requires its reason".to_owned())
            })?,
        )?;
        Ok(())
    }

    fn verify_delivery_state(&self) -> ProtocolResult<()> {
        if matches!(self.delivery, AgentStreamDelivery::ExternalResource { .. })
            && (self.next_chunk_sequence != 0
                || self.staged_bytes != 0
                || self.staged_content_blocks != 0)
        {
            return Err(ProtocolError::Validation(
                "external Agent stream cannot retain staged chunks".to_owned(),
            ));
        }
        match (&self.delivery, &self.publication_reservation) {
            (
                AgentStreamDelivery::ExternalResource {
                    resolver_binding,
                    content,
                },
                Some(reservation),
            ) => {
                reservation.verify()?;
                if reservation.intent.session_id() != self.session_id
                    || reservation.intent.stream_id() != self.stream_id
                    || reservation.intent.resolver_binding() != resolver_binding
                    || reservation.intent.target() != &self.target
                    || reservation.intent.content() != content
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent stream publication reservation changed its owner, target, or delivery"
                            .to_owned(),
                    ));
                }
            }
            (AgentStreamDelivery::ExternalResource { .. } | AgentStreamDelivery::Staged, None) => {}
            (AgentStreamDelivery::Staged, Some(_)) => {
                return Err(ProtocolError::Validation(
                    "staged Agent stream cannot retain a publication reservation".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

fn agent_stream_final_update_content_digest(
    target: &AgentStreamTarget,
    update: &AgentUpdate,
) -> ProtocolResult<String> {
    let content = match (target, update) {
        (AgentStreamTarget::Message { message_id, role }, AgentUpdate::Message { message, .. })
            if message.message_id == *message_id && message.role == *role =>
        {
            &message.content
        }
        (AgentStreamTarget::Tool { tool_call_id }, AgentUpdate::Tool { tool, .. })
            if tool.tool_call_id == *tool_call_id && tool.status == ToolCallStatus::Completed =>
        {
            tool.output.as_ref().ok_or_else(|| {
                ProtocolError::IdentityMismatch(
                    "Agent stream finalized Tool update lost its output".to_owned(),
                )
            })?
        }
        _ => {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream final update changed its immutable target".to_owned(),
            ));
        }
    };
    canonical_digest(content).map_err(Into::into)
}

/// Revision-pinned exact stream current read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamRead {
    /// Exact `StateRoot` revision read by the query.
    pub revision: String,
    /// Current stream projection, absent when the exact key does not exist.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current: Option<AgentStreamCurrent>,
}

impl AgentStreamRead {
    /// Verify revision pinning and exact Session/stream ownership for one query.
    ///
    /// # Errors
    ///
    /// Returns an error when the query or revision is invalid, or when the
    /// returned stream current does not match the exact queried key.
    pub fn verify_for(&self, query: &AgentStreamQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_read_revision(&self.revision, query.expected_revision.as_ref())?;
        if let Some(current) = &self.current {
            current.verify()?;
            if current.session_id != query.session_id || current.stream_id != query.stream_id {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent stream read changed its exact key".to_owned(),
                ));
            }
        }
        validate_canonical_size("Agent stream read", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// One immutable keyed stream chunk and its append-only order head.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamChunkCurrent {
    /// Owning Session identity.
    pub session_id: String,
    /// Owning stream identity.
    pub stream_id: String,
    /// Exact immutable chunk.
    pub chunk: AgentStreamChunk,
    /// Prior chunk head, absent only for sequence zero.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub previous_head: Option<String>,
    /// Content identity of this complete staged entry.
    pub head: String,
    /// Canonical bytes charged to the bounded staged stream.
    pub canonical_bytes: u64,
    /// Exact Agent command that admitted this chunk.
    pub admitted_by: String,
}

impl AgentStreamChunkCurrent {
    fn new(
        stream: &AgentStreamCurrent,
        command_id: &str,
        chunk: &AgentStreamChunk,
    ) -> ProtocolResult<Self> {
        let canonical_bytes = u64::try_from(cymule_core::canonical_bytes(chunk)?.len())
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        let head = content_id(
            AGENT_STREAM_CHUNK_HEAD_DOMAIN,
            &(
                stream.session_id.as_str(),
                stream.stream_id.as_str(),
                chunk,
                stream.chunk_head.as_deref(),
                command_id,
            ),
        )?;
        let current = Self {
            session_id: stream.session_id.clone(),
            stream_id: stream.stream_id.clone(),
            chunk: chunk.clone(),
            previous_head: stream.chunk_head.clone(),
            head,
            canonical_bytes,
            admitted_by: command_id.to_owned(),
        };
        current.verify()?;
        Ok(current)
    }

    /// Verify exact owner, sequence predecessor, byte charge, and content head.
    ///
    /// # Errors
    ///
    /// Returns an error when the owner, chunk, predecessor, byte charge,
    /// admitting command, or content-derived head is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", &self.session_id)?;
        validate_identity("Agent stream", &self.stream_id)?;
        self.chunk.verify()?;
        validate_sha256("Agent stream chunk command", &self.admitted_by)?;
        if (self.chunk.sequence == 0) != self.previous_head.is_none() {
            return Err(ProtocolError::Validation(
                "Agent stream chunk predecessor does not match its sequence".to_owned(),
            ));
        }
        if let Some(previous_head) = &self.previous_head {
            validate_sha256("Agent previous stream chunk head", previous_head)?;
        }
        let expected_bytes = u64::try_from(cymule_core::canonical_bytes(&self.chunk)?.len())
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        if self.canonical_bytes != expected_bytes {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream chunk byte charge does not match its content".to_owned(),
            ));
        }
        let expected = content_id(
            AGENT_STREAM_CHUNK_HEAD_DOMAIN,
            &(
                self.session_id.as_str(),
                self.stream_id.as_str(),
                &self.chunk,
                self.previous_head.as_deref(),
                self.admitted_by.as_str(),
            ),
        )?;
        if self.head != expected {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream chunk head does not match its content".to_owned(),
            ));
        }
        validate_canonical_size("Agent stream chunk current", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Exact keyed target current required by a stream open or finalization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentStreamTargetSource {
    /// Message alias current for the immutable target identity.
    Message {
        /// Existing message, absent when a new target is required.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        current: Option<AgentMessageCurrent>,
    },
    /// Tool current for the immutable target identity.
    Tool {
        /// Existing tool, required for a tool target.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        current: Option<AgentToolCurrent>,
    },
}

/// Exact Resource lifecycle currents read before an external stream finalization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamResourceSource {
    /// Current physical retention family, absent before its first pin.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub retention: Option<ResourceRetentionCurrent>,
    /// Current exact Agent stream pin, absent before first finalization.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub pin: Option<ResourcePinCurrent>,
}

/// Bounded exact before witness for one stream transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "transition", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentStreamSource {
    /// Before witness for a new stream.
    Open {
        /// Exact Session metadata.
        session: AgentSessionCurrent,
        /// Existing stream current; terminal open requires this to be absent.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        stream: Option<AgentStreamCurrent>,
        /// Exact target alias/current.
        target: AgentStreamTargetSource,
    },
    /// Before witness for one chunk append.
    AppendChunk {
        /// Exact open stream current.
        stream: AgentStreamCurrent,
        /// Existing ordinal chunk, absent for a new append.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        current_chunk: Option<AgentStreamChunkCurrent>,
    },
    /// Before witness for abort.
    Abort {
        /// Exact bounded Session metadata.
        session: AgentSessionCurrent,
        /// Exact open stream current.
        stream: AgentStreamCurrent,
        /// Exact Resource lifecycle source when abort releases a provider-proved
        /// `NotApplied` publication reservation.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        resource: Option<Box<AgentStreamResourceSource>>,
        /// Exact target-claim generation when this Abort releases a reservation.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        target_claim: Option<Box<AgentTargetClaimCurrent>>,
    },
    /// Before witness for finalization.
    Finalize {
        /// Exact Session metadata.
        session: AgentSessionCurrent,
        /// Exact open stream current.
        stream: AgentStreamCurrent,
        /// Ordered bounded staged chunks; empty only for external publication.
        chunks: Vec<AgentStreamChunkCurrent>,
        /// Exact target alias/current.
        target: AgentStreamTargetSource,
        /// Existing final Session update identity; terminal finalization requires absence.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        update: Option<AgentUpdateCurrent>,
        /// Exact Resource lifecycle source for an external publication.
        ///
        /// This is absent during provider preflight because the publication
        /// selects the Resource keys. The final reducer and receipt require
        /// the exact currents read from the command's pinned source revision.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        resource: Option<Box<AgentStreamResourceSource>>,
        /// Exact target-claim generation or proved absence.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        target_claim: Option<Box<AgentTargetClaimCurrent>>,
    },
}

#[derive(Serialize)]
struct AgentStreamPublicationIntentIdentity<'a> {
    intent_version: &'a str,
    source_revision: &'a str,
    source_digest: &'a str,
    session_id: &'a str,
    stream_id: &'a str,
    command_id: &'a str,
    resolver_binding: &'a str,
    target: &'a AgentStreamTarget,
    content: &'a AgentStreamPublicationContent,
}

/// Framework-derived immutable authority for one external stream publication.
///
/// This value has no public constructor. It is serializable so an Unknown
/// outcome can cross a process restart, but every consumer must verify it and
/// exact-match it against the pinned touched-source digest before observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamPublicationIntent {
    intent_version: String,
    intent_id: String,
    source_revision: String,
    source_digest: String,
    session_id: String,
    stream_id: String,
    command_id: String,
    resolver_binding: String,
    target: AgentStreamTarget,
    content: AgentStreamPublicationContent,
}

impl AgentStreamPublicationIntent {
    fn new(
        source_revision: &str,
        source: &AgentStreamSource,
        command_id: &str,
        stream: &AgentStreamCurrent,
        resolver_binding: &str,
        content: &AgentStreamPublicationContent,
    ) -> ProtocolResult<Self> {
        validate_sha256("Agent stream publication source revision", source_revision)?;
        let source_digest = agent_stream_publication_source_digest(source)?;
        let intent_id = content_id(
            AGENT_STREAM_PUBLICATION_INTENT_VERSION,
            &AgentStreamPublicationIntentIdentity {
                intent_version: AGENT_STREAM_PUBLICATION_INTENT_VERSION,
                source_revision,
                source_digest: &source_digest,
                session_id: &stream.session_id,
                stream_id: &stream.stream_id,
                command_id,
                resolver_binding,
                target: &stream.target,
                content,
            },
        )?;
        let intent = Self {
            intent_version: AGENT_STREAM_PUBLICATION_INTENT_VERSION.to_owned(),
            intent_id,
            source_revision: source_revision.to_owned(),
            source_digest,
            session_id: stream.session_id.clone(),
            stream_id: stream.stream_id.clone(),
            command_id: command_id.to_owned(),
            resolver_binding: resolver_binding.to_owned(),
            target: stream.target.clone(),
            content: content.clone(),
        };
        intent.verify()?;
        Ok(intent)
    }

    /// Verify the complete immutable publication authority.
    ///
    /// # Errors
    ///
    /// Returns an error when any owner, binding, content descriptor, or
    /// content-derived intent identity changed.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_agent_version(
            "Agent stream publication intent",
            &self.intent_version,
            AGENT_STREAM_PUBLICATION_INTENT_VERSION,
        )?;
        validate_sha256(
            "Agent stream publication source revision",
            &self.source_revision,
        )?;
        if self.source_digest.len() != 64
            || !self
                .source_digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self
                .source_digest
                .bytes()
                .any(|byte| byte.is_ascii_uppercase())
        {
            return Err(ProtocolError::Validation(
                "Agent stream publication source digest must be lowercase SHA-256 hex".to_owned(),
            ));
        }
        validate_identity("Agent Session", &self.session_id)?;
        validate_identity("Agent stream", &self.stream_id)?;
        validate_sha256("Agent stream publication command", &self.command_id)?;
        validate_identity(
            "Agent stream publication resolver binding",
            &self.resolver_binding,
        )?;
        self.target.verify()?;
        self.content.verify()?;
        external_stream_resource_handle(&self.content)?;
        let expected = content_id(
            AGENT_STREAM_PUBLICATION_INTENT_VERSION,
            &AgentStreamPublicationIntentIdentity {
                intent_version: &self.intent_version,
                source_revision: &self.source_revision,
                source_digest: &self.source_digest,
                session_id: &self.session_id,
                stream_id: &self.stream_id,
                command_id: &self.command_id,
                resolver_binding: &self.resolver_binding,
                target: &self.target,
                content: &self.content,
            },
        )?;
        if self.intent_id != expected {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream publication intent does not match its complete authority".to_owned(),
            ));
        }
        validate_canonical_size(
            "Agent stream publication intent",
            &AgentStreamPublicationIntentIdentity {
                intent_version: &self.intent_version,
                source_revision: &self.source_revision,
                source_digest: &self.source_digest,
                session_id: &self.session_id,
                stream_id: &self.stream_id,
                command_id: &self.command_id,
                resolver_binding: &self.resolver_binding,
                target: &self.target,
                content: &self.content,
            },
            MAX_AGENT_VALUE_BYTES,
        )
    }

    /// Content identity of this complete immutable publication authority.
    #[must_use]
    pub fn intent_id(&self) -> &str {
        &self.intent_id
    }

    /// Exact `StateRoot` revision from which the touched source was derived.
    #[must_use]
    pub fn source_revision(&self) -> &str {
        &self.source_revision
    }

    /// Canonical digest of the complete touched Agent stream source.
    #[must_use]
    pub fn source_digest(&self) -> &str {
        &self.source_digest
    }

    /// Owning Session identity.
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Owning stream identity.
    #[must_use]
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Exact Finalize command identity.
    #[must_use]
    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    /// Exact resolver/provider implementation binding.
    #[must_use]
    pub fn resolver_binding(&self) -> &str {
        &self.resolver_binding
    }

    /// Immutable Session target of the finalized Resource Handle.
    #[must_use]
    pub const fn target(&self) -> &AgentStreamTarget {
        &self.target
    }

    /// Exact content which must be published and read back.
    #[must_use]
    pub const fn content(&self) -> &AgentStreamPublicationContent {
        &self.content
    }

    /// Derive the only semantic Resource Handle this intent may publish.
    ///
    /// # Errors
    ///
    /// Returns an error when the intent or its closed Object descriptor is invalid.
    pub fn resource_handle(&self) -> ProtocolResult<ResourceHandle> {
        self.verify()?;
        external_stream_resource_handle(&self.content)
    }
}

/// Durable phase of one exact external publication attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStreamPublicationReservationPhase {
    /// One freshly acknowledged Store CAS owns exactly one provider publish call.
    DispatchClaimed,
    /// Exact provider observation proved the latest attempt did not apply.
    NotApplied,
}

#[derive(Serialize)]
struct AgentStreamPublicationReservationIdentity<'a> {
    reservation_version: &'a str,
    intent: &'a AgentStreamPublicationIntent,
    resource_pin_receipt: &'a ResourcePinReceipt,
}

/// Durable pre-publication authority for one external Agent stream.
///
/// The stable reservation identity binds the immutable intent and Resource
/// retention obligation. `attempt` and `phase` are the bounded dispatch
/// current: only a fresh CAS into `DispatchClaimed` authorizes one provider
/// call, while reopen observes that exact attempt instead of redispatching it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamPublicationReservation {
    /// Reservation wire generation.
    pub reservation_version: String,
    /// Content identity of the immutable intent and Resource obligation.
    pub reservation_id: String,
    /// Content identity of reservation, attempt, and current phase.
    pub dispatch_id: String,
    /// Framework-derived immutable provider authority.
    pub intent: AgentStreamPublicationIntent,
    /// Exact reserved Resource pin receipt and resulting family count.
    pub resource_pin_receipt: ResourcePinReceipt,
    /// One-based provider dispatch attempt ordinal.
    pub attempt: u64,
    /// Durable dispatch state of the latest attempt.
    pub phase: AgentStreamPublicationReservationPhase,
}

impl AgentStreamPublicationReservation {
    fn new(
        intent: AgentStreamPublicationIntent,
        resource_pin_receipt: ResourcePinReceipt,
    ) -> ProtocolResult<Self> {
        let reservation_id = content_id(
            AGENT_STREAM_PUBLICATION_RESERVATION_VERSION,
            &AgentStreamPublicationReservationIdentity {
                reservation_version: AGENT_STREAM_PUBLICATION_RESERVATION_VERSION,
                intent: &intent,
                resource_pin_receipt: &resource_pin_receipt,
            },
        )?;
        let reservation = Self {
            reservation_version: AGENT_STREAM_PUBLICATION_RESERVATION_VERSION.to_owned(),
            dispatch_id: agent_stream_publication_dispatch_id(
                &intent,
                &resource_pin_receipt,
                1,
                AgentStreamPublicationReservationPhase::DispatchClaimed,
            )?,
            reservation_id,
            intent,
            resource_pin_receipt,
            attempt: 1,
            phase: AgentStreamPublicationReservationPhase::DispatchClaimed,
        };
        reservation.verify()?;
        Ok(reservation)
    }

    /// Verify immutable intent/pin closure and bounded dispatch state.
    ///
    /// # Errors
    ///
    /// Returns an error when the reservation identity, owner, pin, attempt, or
    /// canonical representation changed.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_agent_version(
            "Agent stream publication reservation",
            &self.reservation_version,
            AGENT_STREAM_PUBLICATION_RESERVATION_VERSION,
        )?;
        self.intent.verify()?;
        self.resource_pin_receipt
            .verify()
            .map_err(resource_protocol_error)?;
        let expected_pin = agent_stream_resource_profile_pin_from_intent(&self.intent)?;
        if self.resource_pin_receipt.command_id != self.intent.command_id()
            || self.resource_pin_receipt.pin != expected_pin.pin
            || self.attempt == 0
            || self.attempt > MAX_EXACT_INTEGER
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream publication reservation changed its command, pin, or attempt"
                    .to_owned(),
            ));
        }
        let expected = content_id(
            AGENT_STREAM_PUBLICATION_RESERVATION_VERSION,
            &AgentStreamPublicationReservationIdentity {
                reservation_version: &self.reservation_version,
                intent: &self.intent,
                resource_pin_receipt: &self.resource_pin_receipt,
            },
        )?;
        if self.reservation_id != expected {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream publication reservation does not match its immutable authority"
                    .to_owned(),
            ));
        }
        let expected_dispatch = agent_stream_publication_dispatch_id(
            &self.intent,
            &self.resource_pin_receipt,
            self.attempt,
            self.phase,
        )?;
        if self.dispatch_id != expected_dispatch {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream publication dispatch does not match its attempt and phase".to_owned(),
            ));
        }
        validate_canonical_size(
            "Agent stream publication reservation",
            self,
            MAX_AGENT_VALUE_BYTES,
        )
    }

    /// Advance exact `NotApplied` evidence into one freshly claimable attempt.
    ///
    /// # Errors
    ///
    /// Returns an error unless the latest durable attempt is `NotApplied`.
    pub fn rearm(&self) -> ProtocolResult<Self> {
        self.verify()?;
        if self.phase != AgentStreamPublicationReservationPhase::NotApplied {
            return Err(ProtocolError::Conflict {
                code: "agent_stream_publication_attempt_unresolved".to_owned(),
                message: "Agent stream publication attempt must be observed before redispatch"
                    .to_owned(),
            });
        }
        let mut next = self.clone();
        next.attempt = next.attempt.checked_add(1).ok_or_else(|| {
            ProtocolError::Validation(
                "Agent stream publication attempt ordinal is exhausted".to_owned(),
            )
        })?;
        next.phase = AgentStreamPublicationReservationPhase::DispatchClaimed;
        next.dispatch_id = agent_stream_publication_dispatch_id(
            &next.intent,
            &next.resource_pin_receipt,
            next.attempt,
            next.phase,
        )?;
        next.verify()?;
        Ok(next)
    }

    /// Retain exact `NotApplied` evidence for the latest claimed attempt.
    ///
    /// # Errors
    ///
    /// Returns an error unless this reservation currently owns a dispatch.
    pub fn mark_not_applied(&self) -> ProtocolResult<Self> {
        self.verify()?;
        if self.phase != AgentStreamPublicationReservationPhase::DispatchClaimed {
            return Err(ProtocolError::Conflict {
                code: "agent_stream_publication_not_dispatched".to_owned(),
                message: "Agent stream publication has no claimed attempt to settle".to_owned(),
            });
        }
        let mut next = self.clone();
        next.phase = AgentStreamPublicationReservationPhase::NotApplied;
        next.dispatch_id = agent_stream_publication_dispatch_id(
            &next.intent,
            &next.resource_pin_receipt,
            next.attempt,
            next.phase,
        )?;
        next.verify()?;
        Ok(next)
    }
}

fn agent_stream_publication_dispatch_id(
    intent: &AgentStreamPublicationIntent,
    resource_pin_receipt: &ResourcePinReceipt,
    attempt: u64,
    phase: AgentStreamPublicationReservationPhase,
) -> ProtocolResult<String> {
    content_id(
        AGENT_STREAM_PUBLICATION_DISPATCH_ID_DOMAIN,
        &(intent, resource_pin_receipt, attempt, phase),
    )
    .map_err(Into::into)
}

/// Closed provider observation for one idempotent publication intent.
#[derive(Debug, Clone, PartialEq)]
pub enum AgentStreamPublicationObservation {
    /// The provider published and exactly read back this immutable Resource.
    Published {
        /// Verified provider-neutral publication read back after the write.
        publication: Box<ResourcePublication>,
    },
    /// The provider proved that the intent did not apply.
    NotApplied,
    /// The provider cannot yet prove whether the intent applied.
    Unknown,
}

/// Non-serializable provider product admitted only by the specialized Durable
/// stream-finalization capability.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentStreamPublicationProduct {
    intent: AgentStreamPublicationIntent,
    publication: ResourcePublication,
    profile_pin: ResourceProfilePin,
}

impl AgentStreamPublicationProduct {
    fn new(
        intent: AgentStreamPublicationIntent,
        publication: ResourcePublication,
    ) -> ProtocolResult<Self> {
        intent.verify()?;
        verify_external_stream_publication(&intent, &publication)?;
        validate_canonical_size(
            "Agent stream publication product",
            &publication,
            MAX_AGENT_VALUE_BYTES,
        )?;
        let profile_pin = agent_stream_resource_profile_pin(
            intent.session_id(),
            intent.stream_id(),
            &publication,
        )?;
        Ok(Self {
            intent,
            publication,
            profile_pin,
        })
    }

    /// Borrow the exact framework-derived provider intent.
    #[must_use]
    pub const fn intent(&self) -> &AgentStreamPublicationIntent {
        &self.intent
    }

    /// Borrow the verified immutable publication returned by the pinned provider.
    #[must_use]
    pub const fn publication(&self) -> &ResourcePublication {
        &self.publication
    }

    /// Borrow the exact cross-profile Resource pin derived during preflight.
    #[must_use]
    pub const fn resource_profile_pin(&self) -> &ResourceProfilePin {
        &self.profile_pin
    }
}

/// Closed result of provider publication or exact reconciliation.
#[derive(Debug, Clone, PartialEq)]
pub enum AgentStreamPublicationResult {
    /// The exact intent is applied and read back under this product.
    Published {
        /// Exact `DispatchClaimed` reservation observed before provider I/O.
        dispatch: Box<AgentStreamPublicationReservation>,
        /// Framework-verified non-Serde publication product.
        product: Box<AgentStreamPublicationProduct>,
    },
    /// Exact provider evidence proves the intent did not apply.
    NotApplied {
        /// Exact `DispatchClaimed` reservation observed before provider I/O.
        dispatch: Box<AgentStreamPublicationReservation>,
        /// Original immutable intent which remains safe to invoke idempotently.
        intent: AgentStreamPublicationIntent,
    },
    /// The provider or later Durable CAS cannot prove the terminal outcome.
    Unknown {
        /// Exact `DispatchClaimed` reservation observed before provider I/O.
        dispatch: Box<AgentStreamPublicationReservation>,
        /// Original immutable intent required for exact reconciliation.
        intent: AgentStreamPublicationIntent,
    },
}

impl AgentStreamPublicationResult {
    /// Borrow the exact immutable intent shared by every outcome.
    #[must_use]
    pub const fn intent(&self) -> &AgentStreamPublicationIntent {
        match self {
            Self::Published { product, .. } => product.intent(),
            Self::NotApplied { intent, .. } | Self::Unknown { intent, .. } => intent,
        }
    }

    /// Borrow the exact reservation generation observed before provider I/O.
    #[must_use]
    pub const fn dispatch(&self) -> &AgentStreamPublicationReservation {
        match self {
            Self::Published { dispatch, .. }
            | Self::NotApplied { dispatch, .. }
            | Self::Unknown { dispatch, .. } => dispatch,
        }
    }
}

/// Closed non-terminal acknowledgement of one workspace dispatch submission.
///
/// Neither result proves that the workspace change applied. The retained
/// occurrence remains Started until its exact observer supplies settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentWorkspaceSubmission {
    /// The pinned implementation accepted the exact occurrence for execution.
    Submitted,
    /// Submission or acknowledgement is ambiguous and requires observation.
    Unknown,
}

/// Maximum aggregate raw Artifact bytes returned by one workspace observation.
pub const MAX_AGENT_WORKSPACE_ARTIFACT_BYTES: usize = 4 * 1024 * 1024;

/// Complete non-serializable observation of one retained workspace occurrence.
///
/// Existing parent Artifacts may be reused by exact reference. Every new
/// reference carries its complete immutable record here and is admitted with
/// the owning M1/Agent transition, never through a separate registration.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentWorkspaceObservation {
    /// Closed terminal or still-unknown result from the original observer.
    pub resolution: AgentOccurrenceResolution,
    /// Exact immutable records for newly produced typed Artifact references.
    pub artifacts: Vec<cymule_core::ArtifactRecord>,
}

impl AgentWorkspaceObservation {
    /// Verify exact typed references, immutable bytes, uniqueness, and bounds.
    ///
    /// Durable separately proves that references omitted from `artifacts`
    /// already exist at the same pinned parent root.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong response kind, malformed observation,
    /// duplicate or unrelated record, forged bytes, or an oversized product.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_occurrence_resolution(&self.resolution)?;
        let expected = workspace_observation_artifacts(&self.resolution)?;
        if self.artifacts.len() > MAX_AGENT_VALUE_ENTRIES {
            return Err(ProtocolError::Validation(
                "workspace observation exceeds its Artifact record count".to_owned(),
            ));
        }
        let mut observed = BTreeSet::new();
        let mut bytes = 0_usize;
        for record in &self.artifacts {
            if !expected.contains(&record.reference) || !observed.insert(&record.reference) {
                return Err(ProtocolError::Validation(
                    "workspace observation contains a duplicate or unrelated Artifact record"
                        .to_owned(),
                ));
            }
            bytes = bytes
                .checked_add(record.bytes.len())
                .filter(|bytes| *bytes <= MAX_AGENT_WORKSPACE_ARTIFACT_BYTES)
                .ok_or_else(|| {
                    ProtocolError::Validation(
                        "workspace observation exceeds its aggregate Artifact byte bound"
                            .to_owned(),
                    )
                })?;
            record.validate()?;
        }
        Ok(())
    }
}

fn workspace_observation_artifacts(
    resolution: &AgentOccurrenceResolution,
) -> ProtocolResult<BTreeSet<ArtifactRef>> {
    match resolution {
        AgentOccurrenceResolution::Completed {
            response: AgentHostResponse::Workspace(receipt),
        } => Ok(BTreeSet::from([receipt.evidence.clone()])),
        AgentOccurrenceResolution::Completed { .. } => Err(ProtocolError::Validation(
            "workspace observation requires a typed workspace completion".to_owned(),
        )),
        AgentOccurrenceResolution::NotApplied { evidence }
        | AgentOccurrenceResolution::Unknown { evidence } => Ok(evidence
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Artifact { artifact } => Some(artifact.clone()),
                ContentBlock::Text { .. }
                | ContentBlock::Json { .. }
                | ContentBlock::ResourceHandle { .. } => None,
            })
            .collect()),
    }
}

/// Exact provider registry borrowed by the Durable Agent control façade.
///
/// Implementations must resolve only the immutable binding retained by the
/// supplied stream or workspace occurrence. Mutable defaults, caller-supplied
/// provider products, and generation fallback are outside this contract.
pub trait AgentProviders {
    /// Atomically claim and execute one exact provider dispatch.
    ///
    /// Implementations MUST use a provider-owned durable ledger keyed by
    /// `dispatch.dispatch_id`. Claiming publication and fencing the same
    /// dispatch as `NotApplied` are mutually exclusive terminal transitions.
    /// A call which observes a provider-side `NotApplied` tombstone MUST NOT
    /// issue the world write.
    ///
    /// # Errors
    ///
    /// Returns an error only for malformed authority or a provider protocol
    /// defect. Ambiguous world outcomes return [`AgentStreamPublicationObservation::Unknown`].
    fn publish_agent_stream(
        &mut self,
        dispatch: &AgentStreamPublicationReservation,
    ) -> ProtocolResult<AgentStreamPublicationObservation>;

    /// Reconcile one exact dispatch through the provider-owned ledger.
    ///
    /// Returning `NotApplied` MUST atomically install a terminal tombstone for
    /// `dispatch.dispatch_id` before returning. A concurrent or later publish
    /// call for that dispatch must then return `NotApplied` without issuing the
    /// world write. If publication has already been claimed but is not yet
    /// terminal, this method MUST return `Unknown`.
    ///
    /// # Errors
    ///
    /// Returns an error only when the retained binding cannot observe the exact
    /// intent or violates the closed provider protocol.
    fn reconcile_agent_stream_publication(
        &mut self,
        dispatch: &AgentStreamPublicationReservation,
    ) -> ProtocolResult<AgentStreamPublicationObservation>;

    /// Resolve the exact workspace implementation binding before dispatch.
    ///
    /// # Errors
    ///
    /// Returns an error when the command's execution binding and operation do
    /// not select one exact registered implementation.
    fn bind_agent_workspace(
        &mut self,
        command: &AgentWorkspaceCommand,
    ) -> ProtocolResult<AgentHostBinding>;

    /// Submit one fresh Started occurrence to its exact retained implementation.
    ///
    /// Durable invokes this only once, after the complete Start checkpoint and
    /// its Clock guard acknowledge the owning CAS. Exact Start replay never
    /// invokes this method, and settlement uses the observe-only method below.
    /// The implementation must select the occurrence's complete immutable host
    /// binding, not a current default or another generation.
    ///
    /// # Errors
    ///
    /// Returns an error when submission fails or violates the provider
    /// contract. Since Started is already durable, the caller must preserve it
    /// and report an unknown outcome, never same-request dispatch retry.
    fn dispatch_agent_workspace(
        &mut self,
        command: &AgentWorkspaceCommand,
        occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<AgentWorkspaceSubmission>;

    /// Observe the original binding-pinned workspace dispatch without replay.
    ///
    /// # Errors
    ///
    /// Returns an error when the retained binding cannot establish one closed
    /// completion, not-applied, or still-unknown observation.
    fn observe_agent_workspace(
        &mut self,
        command: &AgentWorkspaceCommand,
        occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<AgentWorkspaceObservation>;
}

/// Invoke the exact external stream provider from one freshly acknowledged
/// durable reservation.
///
/// # Errors
///
/// Returns an error before provider I/O when the reservation does not own a
/// claimed attempt, or after provider I/O when the publication does not match
/// the retained resolver binding.
pub fn execute_agent_stream_publication<P: AgentProviders + ?Sized>(
    reservation: &AgentStreamPublicationReservation,
    providers: &mut P,
) -> ProtocolResult<AgentStreamPublicationResult> {
    reservation.verify()?;
    if reservation.phase != AgentStreamPublicationReservationPhase::DispatchClaimed {
        return Err(ProtocolError::Conflict {
            code: "agent_stream_publication_dispatch_not_claimed".to_owned(),
            message: "Agent stream publication requires a freshly claimed durable attempt"
                .to_owned(),
        });
    }
    let intent = reservation.intent.clone();
    let observation = providers.publish_agent_stream(reservation)?;
    agent_stream_publication_result(reservation.clone(), intent, observation)
}

/// Reconcile one prior external stream publication without redispatch.
///
/// # Errors
///
/// Returns an error before provider I/O when the source and command cannot
/// derive the exact original intent, or when the provider violates the closed
/// observation contract.
pub fn reconcile_agent_stream_publication<P: AgentProviders + ?Sized>(
    reservation: &AgentStreamPublicationReservation,
    expected_intent: &AgentStreamPublicationIntent,
    providers: &mut P,
) -> ProtocolResult<AgentStreamPublicationResult> {
    reservation.verify()?;
    expected_intent.verify()?;
    if reservation.phase != AgentStreamPublicationReservationPhase::DispatchClaimed {
        return Err(ProtocolError::Conflict {
            code: "agent_stream_publication_dispatch_not_claimed".to_owned(),
            message: "Agent stream reconciliation requires the claimed dispatch attempt".to_owned(),
        });
    }
    if &reservation.intent != expected_intent {
        return Err(ProtocolError::Conflict {
            code: "agent_stream_publication_intent_changed".to_owned(),
            message: "Agent stream reservation no longer owns the expected publication intent"
                .to_owned(),
        });
    }
    let observation = providers.reconcile_agent_stream_publication(reservation)?;
    agent_stream_publication_result(reservation.clone(), expected_intent.clone(), observation)
}

fn agent_stream_publication_result(
    dispatch: AgentStreamPublicationReservation,
    intent: AgentStreamPublicationIntent,
    observation: AgentStreamPublicationObservation,
) -> ProtocolResult<AgentStreamPublicationResult> {
    Ok(match observation {
        AgentStreamPublicationObservation::Published { publication } => {
            AgentStreamPublicationResult::Published {
                dispatch: Box::new(dispatch),
                product: Box::new(AgentStreamPublicationProduct::new(intent, *publication)?),
            }
        }
        AgentStreamPublicationObservation::NotApplied => AgentStreamPublicationResult::NotApplied {
            dispatch: Box::new(dispatch),
            intent,
        },
        AgentStreamPublicationObservation::Unknown => AgentStreamPublicationResult::Unknown {
            dispatch: Box::new(dispatch),
            intent,
        },
    })
}

/// Shape-matched bounded postcondition of one stream transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "transition", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentStreamEffect {
    /// Stream metadata was opened.
    Opened {
        /// Session metadata with the exact open-stream index increment.
        session: AgentSessionCurrent,
    },
    /// One exact immutable chunk entry was appended.
    Chunk {
        /// Admitted chunk current.
        current: AgentStreamChunkCurrent,
    },
    /// Stream metadata was terminally aborted.
    Aborted {
        /// Session metadata with the exact open-stream index decrement.
        session: AgentSessionCurrent,
        /// Exact reserved-pin release, present only for an external publication
        /// whose latest provider observation is durably `NotApplied`.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        resource_release_receipt: Option<ResourceReleaseReceipt>,
    },
    /// Stream, Session, and optional Resource publication were finalized atomically.
    Finalized {
        /// Exact bounded Session postcondition.
        session: Box<AgentSessionPostcondition>,
        /// Exact external catalog record, absent for chunked output.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        publication_record: Option<ResourceCatalogRecord>,
        /// Exact Resource pin receipt, absent for chunked output.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        resource_pin_receipt: Option<ResourcePinReceipt>,
        /// Mandatory coupled Agent finalization receipt identity.
        finalization_coupling_id: String,
    },
}

/// Bounded exact postcondition for one stream transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamPostcondition {
    /// Exact resulting stream current.
    pub stream: AgentStreamCurrent,
    /// Shape-matched affected entry and cross-profile postcondition.
    pub effect: AgentStreamEffect,
}

fn validate_sha256(kind: &str, value: &str) -> ProtocolResult<()> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(ProtocolError::Validation(format!(
            "{kind} digest must use sha256"
        )));
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProtocolError::Validation(format!(
            "{kind} digest must contain 64 lowercase hexadecimal characters"
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
            "{kind} must contain 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

/// Closed durable value delivered by one accepted or declined elicitation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentInputResult {
    /// The responder accepted and supplied this schema-validated value.
    Accepted {
        /// Exact accepted JSON value.
        value: Value,
    },
    /// The responder explicitly declined without a value.
    Declined,
}

impl AgentInputResult {
    /// Build the exact Plan wait-result schema for one accepted value schema.
    pub fn schema(value_schema: &Value) -> Value {
        let mut wrapped = serde_json::Map::new();
        if let Some(object) = value_schema.as_object() {
            for keyword in ["$schema", "$id", "$defs"] {
                if let Some(value) = object.get(keyword) {
                    wrapped.insert(keyword.to_owned(), value.clone());
                }
            }
        }
        wrapped.insert(
            "oneOf".to_owned(),
            serde_json::json!([
                {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["outcome", "value"],
                    "properties": {
                        "outcome": {"const": "accepted"},
                        "value": value_schema
                    }
                },
                {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["outcome"],
                    "properties": {
                        "outcome": {"const": "declined"}
                    }
                }
            ]),
        );
        Value::Object(wrapped)
    }

    /// Derive the exact wait result from one verified Session response.
    ///
    /// # Errors
    ///
    /// Returns an error when the response does not satisfy the closed accepted
    /// versus declined input-result contract.
    pub fn from_response(response: &ElicitationResponse) -> ProtocolResult<Self> {
        ElicitationProjection {
            wait_id: "wait:input-result-validation".to_owned(),
            request: ElicitationRequest {
                request_id: response.request_id.clone(),
                schema: Value::Bool(true),
                prompt: Vec::new(),
            },
            response: Some(response.clone()),
        }
        .validate()?;
        Ok(match &response.value {
            Some(value) => Self::Accepted {
                value: value.clone(),
            },
            None => Self::Declined,
        })
    }

    /// Encode the exact canonical wait-result Artifact bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when canonical encoding fails.
    pub fn canonical_bytes(&self) -> ProtocolResult<Vec<u8>> {
        cymule_core::canonical_bytes(self).map_err(ProtocolError::from)
    }
}

/// Bounded exact before witness for one atomic input transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "transition", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentInputSource {
    /// Agent-local state before attaching one pending M1 input Wait.
    Suspend {
        /// Exact bounded Session metadata.
        session: AgentSessionCurrent,
        /// Existing elicitation alias; terminal suspension requires absence.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        elicitation: Option<AgentElicitationCurrent>,
    },
    /// Agent-local state before consuming one pending input suspension.
    Complete {
        /// Exact bounded Session metadata.
        session: AgentSessionCurrent,
        /// Exact pending elicitation current.
        elicitation: AgentElicitationCurrent,
    },
}

/// Result of one atomic durable input suspension or completion checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInputCheckpoint {
    /// Exact bounded Session metadata after the checkpoint.
    pub session: AgentSessionCurrent,
    /// Exact affected elicitation current after the checkpoint.
    pub elicitation: AgentElicitationCurrent,
    /// M1 wait identity correlated with the input request.
    pub wait_id: String,
    /// Typed M1 Wait references committed with the Agent postcondition.
    ///
    /// These content IDs are resolved and exact-matched against the retained
    /// M1 receipts by the Durable Agent facade on commit and every typed read.
    pub wait: AgentInputWaitWitness,
}

/// Closed M1 Wait authority witnessed by one Agent input checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentInputWaitWitness {
    /// The exact pending Wait and Session suspension share one M1 receipt.
    Suspended {
        /// Exact owning Run.
        run_id: String,
        /// Complete structural Wait owner.
        owner: WaitOwner,
        /// Content identity of the retained suspension receipt.
        suspension_receipt_id: String,
    },
    /// The exact result, Wait completion, Continuation, and Session share one M1 CAS.
    Completed {
        /// Exact owning Run.
        run_id: String,
        /// Complete structural Wait owner.
        owner: WaitOwner,
        /// Content identity of the consumed suspension receipt.
        suspension_receipt_id: String,
        /// Content identity of the retained completion receipt.
        completion_receipt_id: String,
        /// Exact response-derived Wait result Artifact.
        result: ArtifactRef,
    },
}

impl AgentInputWaitWitness {
    /// Verify the self-contained structural owner, result, and receipt references.
    ///
    /// This does not resolve the referenced M1 receipts. Callers which treat a
    /// checkpoint as durable authority must use the Durable typed read facade,
    /// which resolves both references and exact-matches their full receipts.
    ///
    /// # Errors
    ///
    /// Returns an error when the Run, structural owner, receipt reference, or
    /// completed Wait-result Artifact is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        let (run_id, owner, suspension_receipt_id, completion) = match self {
            Self::Suspended {
                run_id,
                owner,
                suspension_receipt_id,
            } => (run_id, owner, suspension_receipt_id, None),
            Self::Completed {
                run_id,
                owner,
                suspension_receipt_id,
                completion_receipt_id,
                result,
            } => (
                run_id,
                owner,
                suspension_receipt_id,
                Some((completion_receipt_id, result)),
            ),
        };
        validate_identity("Agent input witness Run", run_id)?;
        owner.verify()?;
        validate_sha256("Agent input suspension receipt", suspension_receipt_id)?;
        if let Some((completion_receipt_id, result)) = completion {
            validate_sha256("Agent input completion receipt", completion_receipt_id)?;
            result.validate()?;
        }
        Ok(())
    }
}

impl AgentInputSource {
    /// Deterministically reduce Agent-local input state around typed M1 receipt references.
    ///
    /// # Errors
    ///
    /// Returns an error when the source, command, or M1 witness does not exactly
    /// match, the transition is illegal, or the derived checkpoint is invalid.
    pub fn reduce(
        &self,
        command_id: &str,
        command: &AgentInputCommand,
        wait: AgentInputWaitWitness,
    ) -> ProtocolResult<AgentInputCheckpoint> {
        validate_sha256("Agent input command", command_id)?;
        command.verify()?;
        wait.verify()?;
        let (mut session, elicitation) = match (command, self, &wait) {
            (
                AgentInputCommand::Suspend { .. },
                Self::Suspend {
                    session,
                    elicitation: None,
                },
                AgentInputWaitWitness::Suspended { .. },
            ) => reduce_input_suspension(session, command_id, command, &wait)?,
            (
                AgentInputCommand::Complete { .. },
                Self::Complete {
                    session,
                    elicitation,
                },
                AgentInputWaitWitness::Completed { .. },
            ) => reduce_input_completion(session, elicitation, command_id, command, &wait)?,
            _ => {
                return Err(ProtocolError::Validation(
                    "Agent input before/wait witness shape does not match its command".to_owned(),
                ));
            }
        };
        session.last_transition = Some(AgentSessionTransitionWitness {
            command_id: command_id.to_owned(),
            kind: AgentSessionTransitionKind::Input,
        });
        session.verify()?;
        elicitation.verify()?;
        let checkpoint = AgentInputCheckpoint {
            session,
            elicitation,
            wait_id: command.wait_id().to_owned(),
            wait,
        };
        checkpoint.verify_for(command)?;
        Ok(checkpoint)
    }
}

fn reduce_input_suspension(
    session: &AgentSessionCurrent,
    command_id: &str,
    command: &AgentInputCommand,
    wait: &AgentInputWaitWitness,
) -> ProtocolResult<(AgentSessionCurrent, AgentElicitationCurrent)> {
    let AgentInputCommand::Suspend {
        session_id,
        wait_id,
        expected_run_id,
        expected_owner,
        request,
    } = command
    else {
        return Err(ProtocolError::Validation(
            "Agent input suspension helper requires a Suspend command".to_owned(),
        ));
    };
    let AgentInputWaitWitness::Suspended { run_id, owner, .. } = wait else {
        return Err(ProtocolError::Validation(
            "Agent input suspension helper requires a suspension witness".to_owned(),
        ));
    };
    session.verify()?;
    if session.state == AgentState::Closed {
        return Err(ProtocolError::IllegalTransition(
            "closed Agent Session cannot suspend for input".to_owned(),
        ));
    }
    if session.session_id.as_str() != session_id
        || run_id != expected_run_id
        || owner != expected_owner
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent input suspension changed its Session, Run, or Wait owner".to_owned(),
        ));
    }
    let mut session = session.clone();
    session.pending_elicitation_count = session
        .pending_elicitation_count
        .checked_add(1)
        .ok_or_else(|| {
            ProtocolError::Validation("Agent pending elicitation count is exhausted".to_owned())
        })?;
    session.state = AgentState::RequiresAction;
    session.stop_reason = None;
    let elicitation = AgentElicitationCurrent {
        session_id: session_id.to_owned(),
        elicitation: ElicitationProjection {
            wait_id: wait_id.to_owned(),
            request: request.clone(),
            response: None,
        },
        admitted_by: command_id.to_owned(),
    };
    Ok((session, elicitation))
}

fn reduce_input_completion(
    session: &AgentSessionCurrent,
    elicitation: &AgentElicitationCurrent,
    command_id: &str,
    command: &AgentInputCommand,
    wait: &AgentInputWaitWitness,
) -> ProtocolResult<(AgentSessionCurrent, AgentElicitationCurrent)> {
    let AgentInputCommand::Complete {
        session_id,
        wait_id,
        expected_run_id,
        expected_owner,
        response,
    } = command
    else {
        return Err(ProtocolError::Validation(
            "Agent input completion helper requires a Complete command".to_owned(),
        ));
    };
    let AgentInputWaitWitness::Completed {
        run_id,
        owner,
        result,
        ..
    } = wait
    else {
        return Err(ProtocolError::Validation(
            "Agent input completion helper requires a completion witness".to_owned(),
        ));
    };
    session.verify()?;
    elicitation.verify()?;
    if session.session_id.as_str() != session_id
        || elicitation.session_id.as_str() != session_id
        || elicitation.elicitation.wait_id.as_str() != wait_id
        || elicitation.elicitation.request.request_id != response.request_id
        || elicitation.elicitation.response.is_some()
        || run_id != expected_run_id
        || owner != expected_owner
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent input completion changed its pending Session, request, Run, or Wait owner"
                .to_owned(),
        ));
    }
    validate_elicitation_response(&elicitation.elicitation.request, response)?;
    let expected_result = cymule_core::artifact_ref(
        WAIT_RESULT_ARTIFACT_KIND,
        &AgentInputResult::from_response(response)?.canonical_bytes()?,
    )?;
    if result != &expected_result {
        return Err(ProtocolError::IdentityMismatch(
            "Agent input completion retained a different Wait result Artifact".to_owned(),
        ));
    }
    let mut session = session.clone();
    session.pending_elicitation_count = session
        .pending_elicitation_count
        .checked_sub(1)
        .ok_or_else(|| {
            ProtocolError::IllegalTransition(
                "Agent pending elicitation count underflowed".to_owned(),
            )
        })?;
    session.state = if session.pending_elicitation_count == 0 {
        AgentState::Running
    } else {
        AgentState::RequiresAction
    };
    session.stop_reason = None;
    let mut elicitation = elicitation.clone();
    elicitation.elicitation.response = Some(response.clone());
    command_id.clone_into(&mut elicitation.admitted_by);
    Ok((session, elicitation))
}

impl AgentInputCheckpoint {
    /// Verify the complete bounded Agent-local postcondition for one input command.
    ///
    /// M1 receipt content IDs remain resolved references; the Durable typed read
    /// facade must resolve and exact-match them before returning this checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the bounded Agent postcondition, Wait witness, or
    /// response-derived result does not exactly match the input command.
    pub fn verify_for(&self, command: &AgentInputCommand) -> ProtocolResult<()> {
        self.session.verify()?;
        self.elicitation.verify()?;
        self.wait.verify()?;
        if self.session.session_id != command.session_id()
            || self.elicitation.session_id != self.session.session_id
            || self.wait_id != command.wait_id()
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent input checkpoint changed its Session or Wait owner".to_owned(),
            ));
        }
        match (command, &self.wait) {
            (
                AgentInputCommand::Suspend {
                    expected_run_id,
                    expected_owner,
                    request,
                    ..
                },
                AgentInputWaitWitness::Suspended { run_id, owner, .. },
            ) if run_id == expected_run_id
                && owner == expected_owner
                && self.elicitation.elicitation.request == *request
                && self.elicitation.elicitation.response.is_none()
                && self.session.state == AgentState::RequiresAction =>
            {
                Ok(())
            }
            (
                AgentInputCommand::Complete {
                    expected_run_id,
                    expected_owner,
                    response,
                    ..
                },
                AgentInputWaitWitness::Completed {
                    run_id,
                    owner,
                    result,
                    ..
                },
            ) if run_id == expected_run_id
                && owner == expected_owner
                && self.elicitation.elicitation.response.as_ref() == Some(response) =>
            {
                let expected_result = cymule_core::artifact_ref(
                    WAIT_RESULT_ARTIFACT_KIND,
                    &AgentInputResult::from_response(response)?.canonical_bytes()?,
                )?;
                if result != &expected_result {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent input checkpoint retained a different Wait result Artifact"
                            .to_owned(),
                    ));
                }
                Ok(())
            }
            _ => Err(ProtocolError::IdentityMismatch(
                "Agent input checkpoint does not match its exact command".to_owned(),
            )),
        }
    }
}

/// Closed current-Clock request for one workspace dispatch claim lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentWorkspaceDispatchLeaseRequest {
    /// Sole claim owner derived from the workspace occurrence.
    pub owner: String,
    /// Exact current-head Clock observation to resolve under the final CAS guard.
    pub clock: ClockObservationRef,
    /// Positive logical lease duration.
    pub ttl: u64,
}

impl AgentWorkspaceDispatchLeaseRequest {
    /// Derive the sole owner and verify one framework-issued Clock lease request.
    ///
    /// # Errors
    ///
    /// Returns an error when identity derivation fails or the Clock/TTL does not
    /// match the workspace Run.
    pub fn new(
        request: &WorkspaceScopeRequest,
        clock: ClockObservationRef,
        ttl: u64,
    ) -> ProtocolResult<Self> {
        let lease = Self {
            owner: agent_workspace_claim_owner_unchecked(request)?,
            clock,
            ttl,
        };
        lease.verify_for(request)?;
        Ok(lease)
    }

    /// Verify exact owner, Run clock scope, and positive bounded duration.
    ///
    /// # Errors
    ///
    /// Returns an error when the owner is not framework-derived, the Clock
    /// reference is invalid or uses another Run scope, or the duration is out
    /// of the shared exact integer range.
    pub fn verify_for(&self, request: &WorkspaceScopeRequest) -> ProtocolResult<()> {
        self.clock.verify().map_err(ProtocolError::from)?;
        if self.owner != agent_workspace_claim_owner_unchecked(request)?
            || self.clock.scope != execution_clock_scope(&request.run_id)?
            || self.ttl == 0
            || self.ttl > MAX_EXACT_INTEGER
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent workspace dispatch lease changed its owner, Clock scope, or duration"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// Caller-owned identity and abstract effect site for one workspace overlay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Framework-issued dispatch claim request, present only for `StartEffect`.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub dispatch_lease: Option<AgentWorkspaceDispatchLeaseRequest>,
}

impl WorkspaceScopeRequest {
    /// Construct the exact provider-facing workspace change.
    pub fn change(&self, commit: bool) -> WorkspaceChange {
        WorkspaceChange {
            change_id: self.change_id.clone(),
            overlay: self.overlay.clone(),
            commit,
        }
    }

    /// Verify the complete semantic owner and referenced overlay Artifact.
    ///
    /// # Errors
    ///
    /// Returns an error when any semantic identity or the overlay Artifact is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
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
            validate_identity(&format!("workspace {kind}"), value)?;
        }
        self.overlay.validate()?;
        if let Some(lease) = &self.dispatch_lease {
            lease.verify_for(self)?;
        }
        Ok(())
    }
}

/// Provider decision whose exact M1 operation pin must be resolved before an
/// Agent workspace occurrence can be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentWorkspaceDecision {
    /// Commit the overlay through a Plan-declared mutating Effect.
    Commit,
    /// Abort the overlay without creating an Effect intent.
    Abort,
}

impl AgentWorkspaceDecision {
    /// Provider-facing decision bit.
    pub const fn commit(self) -> bool {
        matches!(self, Self::Commit)
    }
}

/// Closed read-only request for one exact M1 workspace admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentWorkspaceAdmissionQuery {
    /// Complete semantic workspace owner.
    pub request: WorkspaceScopeRequest,
    /// Provider decision being admitted.
    pub decision: AgentWorkspaceDecision,
    /// Exact revision constraint, absent to pin the current head once.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub expected_revision: Option<String>,
}

impl AgentWorkspaceAdmissionQuery {
    /// Verify the complete owner and optional revision pin.
    ///
    /// # Errors
    ///
    /// Returns an error when the workspace owner or revision constraint is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        self.request.verify()?;
        if self.request.dispatch_lease.is_some() != self.decision.commit() {
            return Err(ProtocolError::Validation(
                "Agent workspace admission lease presence does not match its decision".to_owned(),
            ));
        }
        verify_optional_revision(self.expected_revision.as_ref())
    }
}

/// Revision-pinned M1 workspace admission returned by the Durable Agent view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentWorkspaceAdmissionRead {
    /// Exact physical `StateRoot` revision used to resolve every field.
    pub revision: String,
    /// Exact M1 request which must be passed to the Agent host binding step.
    pub host_request: WorkspaceHostRequest,
    /// Current immutable M1 execution-binding Artifact.
    pub execution_binding: ArtifactRef,
    /// Runtime-derived operation occurrence binding.
    pub operation_occurrence_binding: String,
}

impl AgentWorkspaceAdmissionRead {
    /// Verify revision pinning, semantic owner, decision, and M1 binding shape.
    ///
    /// The Durable reader additionally derives and exact-matches the Effect
    /// intent and execution pin from the current Machine/Continuation state.
    ///
    /// # Errors
    ///
    /// Returns an error when the revision, M1 request, execution binding,
    /// operation occurrence binding, or semantic owner differs from the query.
    pub fn verify_for(&self, query: &AgentWorkspaceAdmissionQuery) -> ProtocolResult<()> {
        query.verify()?;
        verify_read_revision(&self.revision, query.expected_revision.as_ref())?;
        self.host_request.validate()?;
        self.execution_binding
            .validate()
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        if self.execution_binding.kind != EXECUTION_BINDING_ARTIFACT_KIND {
            return Err(ProtocolError::Validation(
                "Agent workspace admission requires an ExecutionBinding Artifact".to_owned(),
            ));
        }
        validate_identity(
            "workspace operation occurrence binding",
            &self.operation_occurrence_binding,
        )?;
        let owner = self.host_request.m1_owner().ok_or_else(|| {
            ProtocolError::Validation(
                "Agent workspace admission requires an M1 scope request".to_owned(),
            )
        })?;
        let expected_change = query.request.change(query.decision.commit());
        if self.host_request.change() != &expected_change
            || owner.run_id != query.request.run_id
            || owner.scope_id != query.request.scope_id
            || owner.invocation_id != query.request.invocation_id
            || owner.site_id != query.request.site_id
            || owner.occurrence_key != query.request.occurrence_key
            || owner.operation != query.request.operation
            || owner.effect_intent_id.is_some() != query.decision.commit()
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent workspace admission changed its semantic owner or decision".to_owned(),
            ));
        }
        validate_canonical_size(
            "Agent workspace admission read",
            self,
            MAX_AGENT_CURRENT_BYTES,
        )
    }
}

/// Non-serializable provider product admitted only by the specialized Durable
/// workspace capability.
///
/// Binding selection and settlement observation are runtime capabilities, not
/// caller-authored persistence input. The resulting occurrence is retained in
/// the receipt, while this product never enters the wire source or command.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentWorkspaceProviderProduct {
    kind: AgentWorkspaceProviderProductKind,
}

#[derive(Debug, Clone, PartialEq)]
enum AgentWorkspaceProviderProductKind {
    /// Exact host implementation binding resolved before dispatch.
    Bound {
        /// Binding returned by the non-Serde host authority.
        binding: AgentHostBinding,
    },
    /// Exact provider observation returned by the binding-pinned settlement authority.
    Observed {
        /// Closed provider/reconciliation result.
        observation: AgentWorkspaceObservation,
    },
}

impl AgentWorkspaceProviderProduct {
    fn bound(binding: AgentHostBinding) -> ProtocolResult<Self> {
        binding.verify()?;
        Ok(Self {
            kind: AgentWorkspaceProviderProductKind::Bound { binding },
        })
    }

    fn observed(observation: AgentWorkspaceObservation) -> ProtocolResult<Self> {
        observation.verify()?;
        Ok(Self {
            kind: AgentWorkspaceProviderProductKind::Observed { observation },
        })
    }

    /// Borrow the verified immutable records supplied by the original observer.
    pub fn artifacts(&self) -> &[cymule_core::ArtifactRecord] {
        match &self.kind {
            AgentWorkspaceProviderProductKind::Bound { .. } => &[],
            AgentWorkspaceProviderProductKind::Observed { observation } => &observation.artifacts,
        }
    }

    /// Exact typed references whose bytes must exist in the parent or product.
    ///
    /// # Errors
    ///
    /// Returns an error when the retained observation has an invalid response kind.
    pub fn required_artifacts(&self) -> ProtocolResult<BTreeSet<ArtifactRef>> {
        match &self.kind {
            AgentWorkspaceProviderProductKind::Bound { .. } => Ok(BTreeSet::new()),
            AgentWorkspaceProviderProductKind::Observed { observation } => {
                workspace_observation_artifacts(&observation.resolution)
            }
        }
    }
}

/// Invoke the exact workspace provider after command/source-only preflight.
///
/// Start phases resolve one immutable host binding. Settlement phases query the
/// retained binding without replaying the original operation. The returned
/// product is non-Serde and can be consumed only by the workspace reducer.
///
/// # Errors
///
/// Returns an error before provider I/O when the command/source is stale,
/// mismatched, closed, or not in an executable phase, or when the exact
/// provider binding cannot return one valid closed result.
pub fn execute_agent_workspace_provider<P: AgentProviders + ?Sized>(
    source: &AgentWorkspaceSource,
    command: &AgentCommand,
    providers: &mut P,
) -> ProtocolResult<AgentWorkspaceProviderProduct> {
    command.verify()?;
    let AgentCommandAction::Workspace(workspace) = &command.action else {
        return Err(ProtocolError::Validation(
            "Agent workspace provider requires a Workspace command".to_owned(),
        ));
    };
    preflight_agent_workspace_provider(source, workspace)?;
    match workspace.as_ref() {
        AgentWorkspaceCommand::StartEffect { .. } | AgentWorkspaceCommand::StartAbort { .. } => {
            AgentWorkspaceProviderProduct::bound(providers.bind_agent_workspace(workspace)?)
        }
        AgentWorkspaceCommand::SettleEffect { .. } | AgentWorkspaceCommand::SettleAbort { .. } => {
            let occurrence = &source
                .occurrence
                .current
                .as_ref()
                .ok_or_else(|| {
                    ProtocolError::IllegalTransition(
                        "Agent workspace settlement lost its current occurrence".to_owned(),
                    )
                })?
                .occurrence;
            AgentWorkspaceProviderProduct::observed(
                providers.observe_agent_workspace(workspace, occurrence)?,
            )
        }
    }
}

fn preflight_agent_workspace_provider(
    source: &AgentWorkspaceSource,
    command: &AgentWorkspaceCommand,
) -> ProtocolResult<()> {
    command.verify()?;
    source.occurrence.session.verify()?;
    if source.occurrence.session.session_id != command.request().session_id
        || source.occurrence.session.state == AgentState::Closed
    {
        return Err(ProtocolError::IllegalTransition(
            "Agent workspace provider requires its exact non-closed Session current".to_owned(),
        ));
    }
    match (command, &source.occurrence.current) {
        (
            AgentWorkspaceCommand::StartEffect {
                request,
                effect_intent_id,
                ..
            },
            None,
        ) => {
            workspace_host_request(request, true, Some(effect_intent_id.clone()))?;
            verify_new_workspace_occurrence_capacity(&source.occurrence.session)
        }
        (AgentWorkspaceCommand::StartAbort { request, .. }, None) => {
            workspace_host_request(request, false, None)?;
            verify_new_workspace_occurrence_capacity(&source.occurrence.session)
        }
        (AgentWorkspaceCommand::SettleEffect { request }, Some(current)) => {
            preflight_workspace_settlement(current, request, true)
        }
        (AgentWorkspaceCommand::SettleAbort { request }, Some(current)) => {
            preflight_workspace_settlement(current, request, false)
        }
        _ => Err(ProtocolError::IllegalTransition(
            "Agent workspace provider phase does not match its occurrence current".to_owned(),
        )),
    }
}

fn verify_new_workspace_occurrence_capacity(session: &AgentSessionCurrent) -> ProtocolResult<()> {
    if session.next_occurrence_sequence == MAX_EXACT_INTEGER
        || session.unresolved_occurrence_count == MAX_EXACT_INTEGER
    {
        return Err(ProtocolError::Validation(
            "Agent workspace occurrence capacity is exhausted".to_owned(),
        ));
    }
    Ok(())
}

fn preflight_workspace_settlement(
    current: &AgentOccurrenceCurrent,
    request: &WorkspaceScopeRequest,
    commit: bool,
) -> ProtocolResult<()> {
    current.verify()?;
    if !matches!(
        current.occurrence.state,
        AgentHostOccurrenceState::Started | AgentHostOccurrenceState::Unknown
    ) {
        return Err(ProtocolError::IllegalTransition(
            "Agent workspace settlement requires a Started or Unknown occurrence".to_owned(),
        ));
    }
    verify_workspace_terminal_body_capacity(&current.occurrence)?;
    verify_workspace_occurrence_owner(&current.occurrence, request, commit)
}

fn verify_workspace_terminal_body_capacity(occurrence: &AgentHostOccurrence) -> ProtocolResult<()> {
    if occurrence.is_terminal() {
        return Ok(());
    }
    let AgentHostRequest::Workspace(request) = &occurrence.request else {
        return Err(ProtocolError::Validation(
            "workspace terminal capacity requires its exact Workspace request".to_owned(),
        ));
    };
    // Artifact kinds contain only unescaped ASCII. The longest admitted kind
    // therefore gives the exact maximum encoded ArtifactRef width; its other
    // fields have one frozen version and one fixed-width content identity.
    let suffix = "/1";
    let kind = format!(
        "{}{suffix}",
        "a".repeat(cymule_core::MAX_ARTIFACT_KIND_BYTES - suffix.len()),
    );
    let evidence = cymule_core::artifact_ref(kind, &[])?;
    let mut capacity_probe = occurrence.clone();
    capacity_probe.state = AgentHostOccurrenceState::Completed;
    capacity_probe.failure = None;
    capacity_probe.response = Some(AgentHostResponse::Workspace(WorkspaceReceipt {
        change_id: request.change().change_id.clone(),
        committed: request.change().commit,
        evidence,
        occurrence_binding: occurrence.occurrence_binding.binding_id().to_owned(),
    }));
    // This probe is only a typed size calculation. It is never a provider
    // product, a material admission, or a receipt returned for persistence.
    validate_canonical_size(
        "Agent workspace terminal occurrence capacity",
        &capacity_probe,
        MAX_AGENT_VALUE_BYTES,
    )
}

fn validate_occurrence_resolution(resolution: &AgentOccurrenceResolution) -> ProtocolResult<()> {
    match resolution {
        AgentOccurrenceResolution::Completed { response } => response.validate_content(),
        AgentOccurrenceResolution::NotApplied { evidence }
        | AgentOccurrenceResolution::Unknown { evidence } => validate_content_blocks(evidence),
    }
}

/// Bounded exact Agent-local before witness for one workspace transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentWorkspaceSource {
    /// Exact Session and occurrence current before the transition.
    pub occurrence: AgentOccurrenceSource,
}

/// Durable result of a workspace scope decision or reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentWorkspaceM1Witness {
    /// Exact owning Run retained by the M1 receipt.
    pub run_id: String,
    /// Exact owning scope retained by the M1 receipt.
    pub scope_id: String,
    /// Exact terminal M1 phase performed with the Agent transition.
    pub phase: AgentWorkspaceCommandPhase,
    /// Digest of the exact post-transition Continuation projection.
    pub continuation_digest: String,
    /// Structural mutating Effect intent, absent for abort transitions.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub effect_intent_id: Option<String>,
    /// Scope-transferred obligation, absent for abort transitions.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub obligation_id: Option<String>,
    /// Exact closed M1 workspace receipt resolved by the Durable Agent facade.
    pub m1_receipt_id: String,
}

impl AgentWorkspaceM1Witness {
    /// Verify structural command/occurrence coupling for one resolved M1 receipt reference.
    ///
    /// This method does not resolve `m1_receipt_id`. An authoritative Durable
    /// commit or read must resolve that receipt and exact-match its complete
    /// scope, Effect, outbox, obligation, lease, and Continuation transition.
    ///
    /// # Errors
    ///
    /// Returns an error when any receipt reference, semantic owner, phase,
    /// Effect intent, obligation, or Continuation digest is inconsistent.
    pub fn verify_for(
        &self,
        command: &AgentWorkspaceCommand,
        occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<()> {
        validate_identity("Agent workspace witness Run", &self.run_id)?;
        validate_identity("Agent workspace witness scope", &self.scope_id)?;
        validate_sha256(
            "Agent workspace witness Continuation",
            &self.continuation_digest,
        )?;
        validate_sha256("Agent workspace M1 receipt", &self.m1_receipt_id)?;
        let request = command.request();
        let AgentHostRequest::Workspace(host_request) = &occurrence.request else {
            return Err(ProtocolError::IdentityMismatch(
                "Agent workspace M1 witness lost its typed occurrence".to_owned(),
            ));
        };
        let owner = host_request.m1_owner().ok_or_else(|| {
            ProtocolError::IdentityMismatch(
                "Agent workspace M1 witness lost its structural owner".to_owned(),
            )
        })?;
        let effect_intent_id = owner.effect_intent_id.clone();
        let obligation_id = effect_intent_id
            .as_deref()
            .map(cymule_core::effect_obligation_id)
            .transpose()?;
        if self.run_id != request.run_id
            || self.scope_id != request.scope_id
            || self.phase != workspace_checkpoint_phase(command, occurrence)?
            || self.effect_intent_id != effect_intent_id
            || self.obligation_id != obligation_id
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent workspace M1 witness changed its exact command closure".to_owned(),
            ));
        }
        validate_canonical_size("Agent workspace M1 witness", self, MAX_AGENT_CURRENT_BYTES)
    }
}

/// Durable result of a workspace scope decision or reconciliation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceScopeCheckpoint {
    /// Exact bounded Session and occurrence postcondition.
    pub occurrence: AgentOccurrencePostcondition,
    /// Retained provider receipt, when the provider produced one.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub receipt: Option<WorkspaceReceipt>,
    /// Exact bounded resolved reference to the coupled M1 transition.
    pub m1: AgentWorkspaceM1Witness,
}

impl AgentWorkspaceSource {
    /// Reduce a workspace transition with the non-Serde product returned by
    /// the framework-owned, binding-pinned provider authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the command, provider product, exact source, or M1
    /// witness does not match, or the derived checkpoint is invalid.
    pub fn reduce_with_provider(
        &self,
        command_id: &str,
        command: &AgentWorkspaceCommand,
        product: &AgentWorkspaceProviderProduct,
        m1: AgentWorkspaceM1Witness,
    ) -> ProtocolResult<WorkspaceScopeCheckpoint> {
        validate_sha256("Agent workspace command", command_id)?;
        command.verify()?;
        let authority_occurrence = self.preview_occurrence(command, product)?;
        let occurrence = self.reduce_occurrence(command_id, command, &authority_occurrence)?;
        let AgentHostRequest::Workspace(host_request) = &authority_occurrence.request else {
            return Err(ProtocolError::Validation(
                "workspace command lost its typed request".to_owned(),
            ));
        };
        let effect_intent_id = host_request
            .m1_owner()
            .and_then(|owner| owner.effect_intent_id.clone());
        let receipt = match command {
            AgentWorkspaceCommand::StartEffect { .. } => {
                effect_intent_id.ok_or_else(|| {
                    ProtocolError::Validation(
                        "workspace Effect start lost its intent identity".to_owned(),
                    )
                })?;
                None
            }
            AgentWorkspaceCommand::SettleEffect { .. } => {
                effect_intent_id.ok_or_else(|| {
                    ProtocolError::Validation(
                        "workspace Effect settlement lost its intent identity".to_owned(),
                    )
                })?;
                match workspace_resolution(&authority_occurrence)? {
                    AgentWorkspaceResolution::Applied => {
                        let Some(AgentHostResponse::Workspace(receipt)) =
                            &authority_occurrence.response
                        else {
                            return Err(ProtocolError::Validation(
                                "applied workspace settlement lost its typed receipt".to_owned(),
                            ));
                        };
                        Some(receipt.clone())
                    }
                    AgentWorkspaceResolution::NotApplied | AgentWorkspaceResolution::Unknown => {
                        None
                    }
                }
            }
            AgentWorkspaceCommand::StartAbort { .. } => None,
            AgentWorkspaceCommand::SettleAbort { .. } => {
                match workspace_resolution(&authority_occurrence)? {
                    AgentWorkspaceResolution::Applied => {
                        let Some(AgentHostResponse::Workspace(receipt)) =
                            &authority_occurrence.response
                        else {
                            return Err(ProtocolError::Validation(
                                "applied workspace abort settlement lost its typed receipt"
                                    .to_owned(),
                            ));
                        };
                        Some(receipt.clone())
                    }
                    AgentWorkspaceResolution::NotApplied | AgentWorkspaceResolution::Unknown => {
                        None
                    }
                }
            }
        };
        let checkpoint = WorkspaceScopeCheckpoint {
            occurrence,
            receipt,
            m1,
        };
        checkpoint.verify_for(command)?;
        Ok(checkpoint)
    }

    /// Preview the exact occurrence selected by one binding or observation
    /// product before Durable constructs its real coupled M1 receipt.
    ///
    /// This pure value performs no dispatch and cannot authorize persistence.
    /// The final workspace reducer uses this same path after M1 preparation.
    ///
    /// # Errors
    ///
    /// Returns an error when the source, command, binding, or observation does
    /// not describe the exact closed workspace lifecycle transition.
    pub fn preview_occurrence(
        &self,
        command: &AgentWorkspaceCommand,
        product: &AgentWorkspaceProviderProduct,
    ) -> ProtocolResult<AgentHostOccurrence> {
        preflight_agent_workspace_provider(self, command)?;
        let occurrence = match (command, &product.kind, &self.occurrence.current) {
            (
                AgentWorkspaceCommand::StartEffect {
                    request,
                    effect_intent_id,
                    execution_binding,
                    operation_occurrence_binding,
                },
                AgentWorkspaceProviderProductKind::Bound { binding },
                None,
            ) => {
                binding.verify_m1_effect_operation(
                    execution_binding,
                    &request.operation,
                    operation_occurrence_binding,
                )?;
                let host_request =
                    workspace_host_request(request, true, Some(effect_intent_id.clone()))?;
                AgentHostOccurrence::prepare(
                    &request.occurrence_id,
                    &request.session_id,
                    AgentHostRequest::Workspace(host_request),
                    binding.clone(),
                )?
                .start()
            }
            (
                AgentWorkspaceCommand::StartAbort {
                    request,
                    execution_binding,
                    operation_occurrence_binding,
                },
                AgentWorkspaceProviderProductKind::Bound { binding },
                None,
            ) => {
                binding.verify_m1_effect_operation(
                    execution_binding,
                    &request.operation,
                    operation_occurrence_binding,
                )?;
                let host_request = workspace_host_request(request, false, None)?;
                AgentHostOccurrence::prepare(
                    &request.occurrence_id,
                    &request.session_id,
                    AgentHostRequest::Workspace(host_request),
                    binding.clone(),
                )?
                .start()
            }
            (
                AgentWorkspaceCommand::SettleEffect { request }
                | AgentWorkspaceCommand::SettleAbort { request },
                AgentWorkspaceProviderProductKind::Observed { observation },
                Some(current),
            ) => {
                verify_workspace_occurrence_owner(
                    &current.occurrence,
                    request,
                    matches!(command, AgentWorkspaceCommand::SettleEffect { .. }),
                )?;
                match &observation.resolution {
                    AgentOccurrenceResolution::Completed { response } => {
                        current.occurrence.complete(response.clone())
                    }
                    AgentOccurrenceResolution::NotApplied { evidence } => {
                        current.occurrence.mark_not_applied(evidence.clone())
                    }
                    AgentOccurrenceResolution::Unknown { evidence }
                        if matches!(
                            current.occurrence.state,
                            AgentHostOccurrenceState::Started | AgentHostOccurrenceState::Unknown
                        ) =>
                    {
                        current.occurrence.mark_unknown_with_evidence(
                            "workspace provider outcome remains unknown",
                            evidence.clone(),
                        )
                    }
                    AgentOccurrenceResolution::Unknown { .. } => {
                        Err(ProtocolError::IllegalTransition(
                            "workspace unknown observation requires a Started or Unknown occurrence"
                                .to_owned(),
                        ))
                    }
                }
            }
            _ => Err(ProtocolError::Validation(
                "workspace authority shape does not match its command and current occurrence"
                    .to_owned(),
            )),
        }?;
        verify_workspace_terminal_body_capacity(&occurrence)?;
        Ok(occurrence)
    }

    fn verify_postcondition(
        &self,
        command_id: &str,
        command: &AgentWorkspaceCommand,
        checkpoint: &WorkspaceScopeCheckpoint,
    ) -> ProtocolResult<()> {
        checkpoint.verify_for(command)?;
        let occurrence = &checkpoint.occurrence.current.occurrence;
        let product = match command {
            AgentWorkspaceCommand::StartEffect { .. }
            | AgentWorkspaceCommand::StartAbort { .. } => {
                AgentWorkspaceProviderProduct::bound(occurrence.occurrence_binding.clone())?
            }
            AgentWorkspaceCommand::SettleEffect { .. }
            | AgentWorkspaceCommand::SettleAbort { .. } => {
                AgentWorkspaceProviderProduct::observed(AgentWorkspaceObservation {
                    resolution: occurrence_resolution(occurrence)?,
                    artifacts: Vec::new(),
                })?
            }
        };
        let expected =
            self.reduce_with_provider(command_id, command, &product, checkpoint.m1.clone())?;
        if &expected != checkpoint {
            return Err(ProtocolError::IdentityMismatch(
                "Agent workspace receipt is not the exact authorized transition".to_owned(),
            ));
        }
        Ok(())
    }

    fn reduce_occurrence(
        &self,
        command_id: &str,
        command: &AgentWorkspaceCommand,
        occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<AgentOccurrencePostcondition> {
        if self.occurrence.current.is_some() {
            let mut postcondition = self.occurrence.reduce_workspace(command_id, occurrence)?;
            postcondition.session.last_transition = Some(AgentSessionTransitionWitness {
                command_id: command_id.to_owned(),
                kind: AgentSessionTransitionKind::Workspace,
            });
            postcondition.session.verify()?;
            return Ok(postcondition);
        }
        if !matches!(
            command,
            AgentWorkspaceCommand::StartEffect { .. } | AgentWorkspaceCommand::StartAbort { .. }
        ) {
            return Err(ProtocolError::IllegalTransition(
                "workspace Effect settlement requires an existing exact occurrence current"
                    .to_owned(),
            ));
        }
        self.occurrence.session.verify()?;
        if occurrence.session_id != self.occurrence.session.session_id {
            return Err(ProtocolError::IdentityMismatch(
                "workspace occurrence escaped its bounded Session source".to_owned(),
            ));
        }
        let mut session = self.occurrence.session.clone();
        let ordinal = session.next_occurrence_sequence;
        session.next_occurrence_sequence = session
            .next_occurrence_sequence
            .checked_add(1)
            .ok_or_else(|| {
                ProtocolError::Validation("Agent occurrence sequence is exhausted".to_owned())
            })?;
        if !occurrence.is_terminal() {
            session.unresolved_occurrence_count = session
                .unresolved_occurrence_count
                .checked_add(1)
                .ok_or_else(|| {
                    ProtocolError::Validation(
                        "Agent unresolved occurrence count is exhausted".to_owned(),
                    )
                })?;
            session.unresolved_occurrence_generation = unresolved_generation(
                &session.unresolved_occurrence_generation,
                "put",
                ordinal,
                occurrence,
            )?;
        }
        session.last_transition = Some(AgentSessionTransitionWitness {
            command_id: command_id.to_owned(),
            kind: AgentSessionTransitionKind::Workspace,
        });
        session.verify()?;
        let postcondition = AgentOccurrencePostcondition {
            session,
            current: AgentOccurrenceCurrent {
                ordinal,
                occurrence: occurrence.clone(),
                admitted_by: command_id.to_owned(),
            },
        };
        postcondition.verify_for(occurrence)?;
        Ok(postcondition)
    }
}

fn occurrence_resolution(
    occurrence: &AgentHostOccurrence,
) -> ProtocolResult<AgentOccurrenceResolution> {
    occurrence.validate()?;
    match occurrence.state {
        AgentHostOccurrenceState::Completed => {
            let response = occurrence.response.clone().ok_or_else(|| {
                ProtocolError::IdentityMismatch(
                    "completed workspace occurrence lost its response".to_owned(),
                )
            })?;
            Ok(AgentOccurrenceResolution::Completed { response })
        }
        AgentHostOccurrenceState::NotApplied => Ok(AgentOccurrenceResolution::NotApplied {
            evidence: occurrence
                .recovery_observations
                .last()
                .expect("verified NotApplied occurrence has a terminal observation")
                .evidence
                .clone(),
        }),
        AgentHostOccurrenceState::Unknown => Ok(AgentOccurrenceResolution::Unknown {
            evidence: occurrence
                .recovery_observations
                .last()
                .map_or_else(Vec::new, |observation| observation.evidence.clone()),
        }),
        AgentHostOccurrenceState::Prepared | AgentHostOccurrenceState::Started => {
            Err(ProtocolError::IllegalTransition(
                "workspace settlement result has no provider observation".to_owned(),
            ))
        }
    }
}

fn workspace_host_request(
    request: &WorkspaceScopeRequest,
    commit: bool,
    effect_intent_id: Option<String>,
) -> ProtocolResult<WorkspaceHostRequest> {
    WorkspaceHostRequest::m1_scope(
        WorkspaceOccurrenceOwner {
            run_id: request.run_id.clone(),
            scope_id: request.scope_id.clone(),
            invocation_id: request.invocation_id.clone(),
            site_id: request.site_id.clone(),
            occurrence_key: request.occurrence_key.clone(),
            operation: request.operation.clone(),
            effect_intent_id,
        },
        request.change(commit),
    )
}

fn verify_workspace_occurrence_owner(
    occurrence: &AgentHostOccurrence,
    request: &WorkspaceScopeRequest,
    commit: bool,
) -> ProtocolResult<()> {
    occurrence.validate()?;
    let AgentHostRequest::Workspace(host_request) = &occurrence.request else {
        return Err(ProtocolError::Validation(
            "workspace authority requires a typed workspace occurrence".to_owned(),
        ));
    };
    let owner = host_request.m1_owner().ok_or_else(|| {
        ProtocolError::Validation("workspace authority requires its complete M1 owner".to_owned())
    })?;
    if occurrence.session_id != request.session_id
        || occurrence.occurrence_id != request.occurrence_id
        || host_request.change() != &request.change(commit)
        || owner.run_id != request.run_id
        || owner.scope_id != request.scope_id
        || owner.invocation_id != request.invocation_id
        || owner.site_id != request.site_id
        || owner.occurrence_key != request.occurrence_key
        || owner.operation != request.operation
        || owner.effect_intent_id.is_some() != commit
    {
        return Err(ProtocolError::IdentityMismatch(
            "workspace occurrence changed its semantic owner or decision".to_owned(),
        ));
    }
    Ok(())
}

fn workspace_resolution(
    occurrence: &AgentHostOccurrence,
) -> ProtocolResult<AgentWorkspaceResolution> {
    match occurrence.state {
        AgentHostOccurrenceState::Completed => Ok(AgentWorkspaceResolution::Applied),
        AgentHostOccurrenceState::NotApplied => Ok(AgentWorkspaceResolution::NotApplied),
        AgentHostOccurrenceState::Unknown => Ok(AgentWorkspaceResolution::Unknown),
        AgentHostOccurrenceState::Prepared | AgentHostOccurrenceState::Started => {
            Err(ProtocolError::IllegalTransition(
                "workspace occurrence has no provider observation to settle".to_owned(),
            ))
        }
    }
}

impl WorkspaceScopeCheckpoint {
    /// Verify the exact bounded Agent-local workspace postcondition.
    ///
    /// # Errors
    ///
    /// Returns an error when the occurrence, receipt, provider outcome, or M1
    /// witness does not exactly match the workspace command.
    pub fn verify_for(&self, command: &AgentWorkspaceCommand) -> ProtocolResult<()> {
        let occurrence = &self.occurrence.current.occurrence;
        self.occurrence.verify_for(occurrence)?;
        verify_workspace_occurrence_owner(
            occurrence,
            command.request(),
            matches!(
                command,
                AgentWorkspaceCommand::StartEffect { .. }
                    | AgentWorkspaceCommand::SettleEffect { .. }
            ),
        )?;
        let AgentHostRequest::Workspace(host_request) = &occurrence.request else {
            return Err(ProtocolError::IdentityMismatch(
                "workspace checkpoint lost its typed request".to_owned(),
            ));
        };
        let effect_intent_id = host_request
            .m1_owner()
            .and_then(|owner| owner.effect_intent_id.as_deref());
        self.m1.verify_for(command, occurrence)?;
        match command {
            AgentWorkspaceCommand::StartEffect {
                effect_intent_id: command_effect_intent_id,
                ..
            } => {
                let intent_id = effect_intent_id.ok_or_else(|| {
                    ProtocolError::IdentityMismatch(
                        "workspace Effect start checkpoint lost its intent".to_owned(),
                    )
                })?;
                if occurrence.state == AgentHostOccurrenceState::Started
                    && intent_id == command_effect_intent_id
                    && self.receipt.is_none()
                {
                    Ok(())
                } else {
                    Err(ProtocolError::IdentityMismatch(
                        "workspace Effect start checkpoint changed its exact closure".to_owned(),
                    ))
                }
            }
            AgentWorkspaceCommand::SettleEffect { .. } => {
                effect_intent_id.ok_or_else(|| {
                    ProtocolError::IdentityMismatch(
                        "workspace Effect settlement checkpoint lost its intent".to_owned(),
                    )
                })?;
                match (workspace_resolution(occurrence)?, &self.receipt) {
                    (
                        AgentWorkspaceResolution::Applied,
                        Some(WorkspaceReceipt {
                            committed: true, ..
                        }),
                    )
                    | (
                        AgentWorkspaceResolution::NotApplied | AgentWorkspaceResolution::Unknown,
                        None,
                    ) => Ok(()),
                    _ => Err(ProtocolError::IdentityMismatch(
                        "workspace Effect settlement changed its provider result".to_owned(),
                    )),
                }
            }
            AgentWorkspaceCommand::StartAbort { .. } => {
                if occurrence.state == AgentHostOccurrenceState::Started && self.receipt.is_none() {
                    Ok(())
                } else {
                    Err(ProtocolError::IdentityMismatch(
                        "workspace abort start changed its exact closure".to_owned(),
                    ))
                }
            }
            AgentWorkspaceCommand::SettleAbort { .. } => {
                match (workspace_resolution(occurrence)?, &self.receipt) {
                    (
                        AgentWorkspaceResolution::Applied,
                        Some(WorkspaceReceipt {
                            committed: false, ..
                        }),
                    )
                    | (
                        AgentWorkspaceResolution::NotApplied | AgentWorkspaceResolution::Unknown,
                        None,
                    ) => Ok(()),
                    _ => Err(ProtocolError::IdentityMismatch(
                        "workspace abort settlement changed its provider result".to_owned(),
                    )),
                }
            }
        }
    }
}

fn workspace_checkpoint_phase(
    command: &AgentWorkspaceCommand,
    occurrence: &AgentHostOccurrence,
) -> ProtocolResult<AgentWorkspaceCommandPhase> {
    Ok(match command {
        AgentWorkspaceCommand::StartEffect { .. } => {
            if occurrence.state != AgentHostOccurrenceState::Started {
                return Err(ProtocolError::IdentityMismatch(
                    "workspace Effect start did not retain a Started occurrence".to_owned(),
                ));
            }
            AgentWorkspaceCommandPhase::StartEffectDispatch
        }
        AgentWorkspaceCommand::StartAbort { .. } => {
            if occurrence.state != AgentHostOccurrenceState::Started {
                return Err(ProtocolError::IdentityMismatch(
                    "workspace abort start did not retain a Started occurrence".to_owned(),
                ));
            }
            AgentWorkspaceCommandPhase::StartAbortDispatch
        }
        AgentWorkspaceCommand::SettleEffect { .. } => match workspace_resolution(occurrence)? {
            AgentWorkspaceResolution::Applied => AgentWorkspaceCommandPhase::SettleEffectApplied,
            AgentWorkspaceResolution::NotApplied => {
                AgentWorkspaceCommandPhase::SettleEffectNotApplied
            }
            AgentWorkspaceResolution::Unknown => AgentWorkspaceCommandPhase::SettleEffectUnknown,
        },
        AgentWorkspaceCommand::SettleAbort { .. } => match workspace_resolution(occurrence)? {
            AgentWorkspaceResolution::Applied => AgentWorkspaceCommandPhase::SettleAbortApplied,
            AgentWorkspaceResolution::NotApplied => {
                AgentWorkspaceCommandPhase::SettleAbortNotApplied
            }
            AgentWorkspaceResolution::Unknown => AgentWorkspaceCommandPhase::SettleAbortUnknown,
        },
    })
}

/// Closed Agent persistence command generation.
pub const AGENT_COMMAND_VERSION: &str = "cymule.agent-command/4";
/// Closed Agent persistence receipt generation.
pub const AGENT_COMMAND_RECEIPT_VERSION: &str = "cymule.agent-command-receipt/6";

const AGENT_COMMAND_ID_DOMAIN: &str = "cymule.agent-command-id/2";
const AGENT_COMMAND_RECEIPT_ID_DOMAIN: &str = "cymule.agent-command-receipt-id/4";
const AGENT_UPDATE_KEY_DOMAIN: &str = "cymule.agent-update-key/1";
const AGENT_MESSAGE_KEY_DOMAIN: &str = "cymule.agent-message-key/1";
const AGENT_TOOL_KEY_DOMAIN: &str = "cymule.agent-tool-key/1";
const AGENT_ELICITATION_KEY_DOMAIN: &str = "cymule.agent-elicitation-key/1";
const AGENT_OCCURRENCE_KEY_DOMAIN: &str = "cymule.agent-occurrence-key/1";
const AGENT_STREAM_KEY_DOMAIN: &str = "cymule.agent-stream-key/1";
const AGENT_STREAM_CHUNK_KEY_DOMAIN: &str = "cymule.agent-stream-chunk-key/1";
const AGENT_STREAM_FINAL_UPDATE_ID_DOMAIN: &str = "cymule.agent-stream-final-update-id/1";
const AGENT_STREAM_PUBLICATION_NAMESPACE: &str = "cymule.agent-stream-publication/1";
const AGENT_STREAM_FINALIZATION_COUPLING_ID_DOMAIN: &str =
    "cymule.agent-stream-finalization-coupling-id/1";
const AGENT_WORKSPACE_CLAIM_OWNER_ID_DOMAIN: &str = "cymule.agent-workspace-claim-owner-id/1";

/// Validate and return the raw `StateRoot` key for one Session metadata current.
///
/// # Errors
///
/// Returns an error when the Session identity is invalid.
pub fn agent_session_key(session_id: &str) -> ProtocolResult<String> {
    validate_identity("Agent Session", session_id)?;
    Ok(session_id.to_owned())
}

/// Validate and return the shared raw key for one command and its receipt.
///
/// # Errors
///
/// Returns an error when the command identity is not canonical.
pub fn agent_command_key(command_id: &str) -> ProtocolResult<String> {
    validate_sha256("Agent command", command_id)?;
    Ok(command_id.to_owned())
}

/// Derive the exact keyed update alias within one Session.
///
/// # Errors
///
/// Returns an error when either identity is invalid or key derivation fails.
pub fn agent_update_key(session_id: &str, update_id: &str) -> ProtocolResult<String> {
    agent_pair_key(AGENT_UPDATE_KEY_DOMAIN, "update", session_id, update_id)
}

/// Derive the exact immutable message key within one Session.
///
/// # Errors
///
/// Returns an error when either identity is invalid or key derivation fails.
pub fn agent_message_key(session_id: &str, message_id: &str) -> ProtocolResult<String> {
    agent_pair_key(AGENT_MESSAGE_KEY_DOMAIN, "message", session_id, message_id)
}

/// Derive the exact tool key within one Session.
///
/// # Errors
///
/// Returns an error when either identity is invalid or key derivation fails.
pub fn agent_tool_key(session_id: &str, tool_call_id: &str) -> ProtocolResult<String> {
    agent_pair_key(AGENT_TOOL_KEY_DOMAIN, "tool", session_id, tool_call_id)
}

/// Derive the exact elicitation key within one Session.
///
/// # Errors
///
/// Returns an error when either identity is invalid or key derivation fails.
pub fn agent_elicitation_key(session_id: &str, request_id: &str) -> ProtocolResult<String> {
    agent_pair_key(
        AGENT_ELICITATION_KEY_DOMAIN,
        "elicitation",
        session_id,
        request_id,
    )
}

/// Derive the exact occurrence key within one Session.
///
/// # Errors
///
/// Returns an error when either identity is invalid or key derivation fails.
pub fn agent_occurrence_key(session_id: &str, occurrence_id: &str) -> ProtocolResult<String> {
    agent_pair_key(
        AGENT_OCCURRENCE_KEY_DOMAIN,
        "occurrence",
        session_id,
        occurrence_id,
    )
}

/// Derive the exact stream key within one Session.
///
/// # Errors
///
/// Returns an error when either identity is invalid or key derivation fails.
pub fn agent_stream_key(session_id: &str, stream_id: &str) -> ProtocolResult<String> {
    agent_pair_key(AGENT_STREAM_KEY_DOMAIN, "stream", session_id, stream_id)
}

/// Derive the exact immutable chunk key within one Agent stream.
///
/// # Errors
///
/// Returns an error when either identity or sequence is invalid, or key derivation fails.
pub fn agent_stream_chunk_key(
    session_id: &str,
    stream_id: &str,
    sequence: u64,
) -> ProtocolResult<String> {
    validate_identity("Agent Session", session_id)?;
    validate_identity("Agent stream", stream_id)?;
    if sequence > MAX_EXACT_INTEGER {
        return Err(ProtocolError::Validation(
            "Agent stream chunk sequence exceeds the exact integer range".to_owned(),
        ));
    }
    content_id(
        AGENT_STREAM_CHUNK_KEY_DOMAIN,
        &(session_id, stream_id, sequence),
    )
    .map_err(Into::into)
}

fn agent_pair_key(
    domain: &str,
    kind: &str,
    session_id: &str,
    local_id: &str,
) -> ProtocolResult<String> {
    validate_identity("Agent Session", session_id)?;
    validate_identity(&format!("Agent {kind}"), local_id)?;
    content_id(domain, &(session_id, local_id)).map_err(Into::into)
}

/// One closed Agent profile persistence command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCommand {
    /// Frozen command generation.
    pub command_version: String,
    /// Content identity of the complete closed action.
    pub command_id: String,
    /// Exact current `StateRoot` revision over which the bounded source is resolved.
    pub source_revision: String,
    /// Exact typed action admitted by the Durable facade.
    pub action: AgentCommandAction,
}

impl AgentCommand {
    /// Seal one typed action under its complete content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the source revision or action is invalid, or the
    /// content-derived command identity cannot be produced.
    pub fn new(
        source_revision: impl Into<String>,
        action: AgentCommandAction,
    ) -> ProtocolResult<Self> {
        let source_revision = source_revision.into();
        validate_sha256("Agent command source revision", &source_revision)?;
        action.verify()?;
        let command_id = agent_command_id(&source_revision, &action)?;
        Ok(Self {
            command_version: AGENT_COMMAND_VERSION.to_owned(),
            command_id,
            source_revision,
            action,
        })
    }

    /// Verify version, content identity, and all command-local action invariants.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, revision, action, identity, or bounded
    /// command representation is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        require_agent_version(
            "Agent command",
            &self.command_version,
            AGENT_COMMAND_VERSION,
        )?;
        validate_sha256("Agent command source revision", &self.source_revision)?;
        self.action.verify()?;
        let expected = agent_command_id(&self.source_revision, &self.action)?;
        if self.command_id != expected {
            return Err(ProtocolError::IdentityMismatch(format!(
                "Agent command {} does not match {expected}",
                self.command_id
            )));
        }
        validate_canonical_size("Agent command", self, MAX_AGENT_COMMAND_BYTES)
    }
}

fn agent_command_id(source_revision: &str, action: &AgentCommandAction) -> ProtocolResult<String> {
    content_id(
        AGENT_COMMAND_ID_DOMAIN,
        &(AGENT_COMMAND_VERSION, source_revision, action),
    )
    .map_err(Into::into)
}

/// Bounded exact before witness resolved at one command's source revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "profile",
    content = "source",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AgentCommandSource {
    /// Session metadata, update identity, and the only affected keyed entry.
    Session {
        /// Exact bounded Session metadata.
        session: AgentSessionCurrent,
        /// Exact update identity and entry source.
        update: AgentSessionUpdateSource,
    },
    /// Session metadata and exact occurrence current.
    Occurrence(AgentOccurrenceSource),
    /// Exact bounded stream transition source.
    Stream(Box<AgentStreamSource>),
    /// Exact bounded input transition source.
    Input(AgentInputSource),
    /// Exact bounded workspace transition source.
    Workspace(AgentWorkspaceSource),
}

/// Complete closed union of Agent persistence authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "profile",
    content = "command",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AgentCommandAction {
    /// Append one exact Session update.
    SessionUpdate {
        /// Owning Session identity.
        session_id: String,
        /// Exact idempotent update.
        update: AgentUpdate,
    },
    /// Append one exact host-occurrence lifecycle snapshot.
    Occurrence {
        /// Exact occurrence snapshot.
        occurrence: Box<AgentHostOccurrence>,
    },
    /// Apply one stream lifecycle command.
    Stream(AgentStreamCommand),
    /// Apply one Plan-owned input command.
    Input(AgentInputCommand),
    /// Apply one workspace scope/effect command.
    Workspace(Box<AgentWorkspaceCommand>),
}

impl AgentCommandAction {
    /// Verify every self-contained field without consulting durable state.
    ///
    /// # Errors
    ///
    /// Returns an error when an action is malformed or attempts a mutation
    /// reserved for a more specific composite authority.
    pub fn verify(&self) -> ProtocolResult<()> {
        match self {
            Self::SessionUpdate { session_id, update } => {
                validate_identity("Agent Session", session_id)?;
                if matches!(update, AgentUpdate::Elicitation { .. }) {
                    return Err(ProtocolError::Validation(
                        "Agent elicitation mutation is owned exclusively by AgentInputCommand"
                            .to_owned(),
                    ));
                }
                update.validate_content()
            }
            Self::Occurrence { occurrence } => {
                occurrence.validate()?;
                if occurrence.request.is_m1_workspace() {
                    return Err(ProtocolError::Validation(
                        "M1 workspace occurrence mutation is owned exclusively by AgentWorkspaceCommand"
                            .to_owned(),
                    ));
                }
                Ok(())
            }
            Self::Stream(command) => command.verify(),
            Self::Input(command) => command.verify(),
            Self::Workspace(command) => command.verify(),
        }
    }
}

/// Closed stream lifecycle command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "transition", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentStreamCommand {
    /// Admit one new stream.
    Open {
        /// Owning Session identity.
        session_id: String,
        /// Caller-owned stream identity.
        stream_id: String,
        /// Immutable final target.
        target: AgentStreamTarget,
        /// Immutable delivery authority selected before any output is staged.
        delivery: AgentStreamDelivery,
    },
    /// Append one contiguous chunk.
    AppendChunk {
        /// Owning Session identity.
        session_id: String,
        /// Stream identity.
        stream_id: String,
        /// Exact next chunk.
        chunk: AgentStreamChunk,
    },
    /// Abort one open stream.
    Abort {
        /// Owning Session identity.
        session_id: String,
        /// Stream identity.
        stream_id: String,
        /// Stable non-empty reason.
        reason: String,
    },
    /// Atomically finalize the stream and its Session projection.
    Finalize {
        /// Owning Session identity.
        session_id: String,
        /// Stream identity.
        stream_id: String,
    },
}

impl AgentStreamCommand {
    /// Owning Session identity.
    pub fn session_id(&self) -> &str {
        match self {
            Self::Open { session_id, .. }
            | Self::AppendChunk { session_id, .. }
            | Self::Abort { session_id, .. }
            | Self::Finalize { session_id, .. } => session_id,
        }
    }

    /// Owning stream identity.
    pub fn stream_id(&self) -> &str {
        match self {
            Self::Open { stream_id, .. }
            | Self::AppendChunk { stream_id, .. }
            | Self::Abort { stream_id, .. }
            | Self::Finalize { stream_id, .. } => stream_id,
        }
    }

    /// Verify command-local semantic intent. Provider products never enter the command.
    ///
    /// # Errors
    ///
    /// Returns an error when any owner, target, delivery, chunk, or abort reason is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", self.session_id())?;
        validate_identity("Agent stream", self.stream_id())?;
        match self {
            Self::Open {
                target, delivery, ..
            } => {
                target.verify()?;
                delivery.verify()
            }
            Self::AppendChunk { chunk, .. } => chunk.verify(),
            Self::Abort { reason, .. } => {
                validate_identity("Agent stream abort reason", reason).map_err(Into::into)
            }
            Self::Finalize { .. } => Ok(()),
        }
    }
}

impl AgentStreamSource {
    /// Deterministically reduce one stream command over its bounded exact before witness.
    ///
    /// # Errors
    ///
    /// Returns an error when the command and source do not exactly match, the
    /// transition is illegal, or the derived postcondition is invalid.
    pub fn reduce(
        &self,
        command_id: &str,
        command: &AgentStreamCommand,
    ) -> ProtocolResult<AgentStreamPostcondition> {
        validate_sha256("Agent stream command", command_id)?;
        command.verify()?;
        let postcondition = match command {
            AgentStreamCommand::Open {
                session_id,
                stream_id,
                target,
                delivery,
            } => self.reduce_open(command_id, session_id, stream_id, target, delivery)?,
            AgentStreamCommand::AppendChunk {
                session_id,
                stream_id,
                chunk,
            } => self.reduce_append(command_id, session_id, stream_id, chunk)?,
            AgentStreamCommand::Abort {
                session_id,
                stream_id,
                reason,
            } => self.reduce_abort(command_id, session_id, stream_id, reason)?,
            AgentStreamCommand::Finalize {
                session_id,
                stream_id,
            } => self.reduce_staged_finalization(command_id, session_id, stream_id)?,
        };
        postcondition.verify_for(command)?;
        Ok(postcondition)
    }

    fn reduce_open(
        &self,
        command_id: &str,
        session_id: &str,
        stream_id: &str,
        target: &AgentStreamTarget,
        delivery: &AgentStreamDelivery,
    ) -> ProtocolResult<AgentStreamPostcondition> {
        let Self::Open {
            session,
            stream,
            target: target_source,
        } = self
        else {
            return Err(stream_source_shape_error());
        };
        session.verify()?;
        if session.session_id != session_id
            || session.state == AgentState::Closed
            || stream.is_some()
        {
            return Err(ProtocolError::IllegalTransition(
                "Agent stream open requires an absent current in a non-closed Session".to_owned(),
            ));
        }
        verify_stream_target_source(session_id, target, target_source)?;
        let final_content = match delivery {
            AgentStreamDelivery::Staged => Vec::new(),
            AgentStreamDelivery::ExternalResource { content, .. } => {
                vec![ContentBlock::ResourceHandle {
                    resource: Box::new(external_stream_resource_handle(content)?),
                }]
            }
        };
        let update = stream_finalization_update(
            session_id,
            stream_id,
            target,
            target_source,
            &final_content,
        )?;
        let final_update_bytes = cymule_core::canonical_bytes(&update)?.len() as u64;
        if final_update_bytes > MAX_AGENT_VALUE_BYTES as u64 {
            return Err(ProtocolError::Validation(format!(
                "Agent stream final update occupies {final_update_bytes} canonical bytes; maximum is {MAX_AGENT_VALUE_BYTES}"
            )));
        }
        update.validate_content()?;
        let mut session = session.clone();
        session.open_stream_count = session.open_stream_count.checked_add(1).ok_or_else(|| {
            ProtocolError::Validation("Agent open stream count is exhausted".to_owned())
        })?;
        session.open_stream_generation = open_stream_generation(
            &session.open_stream_generation,
            "put",
            stream_id,
            command_id,
        )?;
        session.last_transition = Some(AgentSessionTransitionWitness {
            command_id: command_id.to_owned(),
            kind: AgentSessionTransitionKind::Stream,
        });
        session.verify()?;
        Ok(AgentStreamPostcondition {
            stream: AgentStreamCurrent {
                stream_id: stream_id.to_owned(),
                session_id: session_id.to_owned(),
                target: target.clone(),
                delivery: delivery.clone(),
                publication_reservation: None,
                state: AgentStreamState::Open,
                next_chunk_sequence: 0,
                chunk_head: None,
                staged_bytes: 0,
                staged_content_blocks: 0,
                final_update_bytes,
                final_update: None,
                content_digest: None,
                abort_reason: None,
                admitted_by: command_id.to_owned(),
            },
            effect: AgentStreamEffect::Opened { session },
        })
    }

    fn reduce_append(
        &self,
        command_id: &str,
        session_id: &str,
        stream_id: &str,
        chunk: &AgentStreamChunk,
    ) -> ProtocolResult<AgentStreamPostcondition> {
        let Self::AppendChunk {
            stream,
            current_chunk,
        } = self
        else {
            return Err(stream_source_shape_error());
        };
        stream.verify()?;
        if stream.session_id != session_id
            || stream.stream_id != stream_id
            || stream.state != AgentStreamState::Open
            || stream.delivery != AgentStreamDelivery::Staged
            || current_chunk.is_some()
            || chunk.sequence != stream.next_chunk_sequence
        {
            return Err(ProtocolError::IllegalTransition(
                "Agent stream append does not match its exact open ordinal".to_owned(),
            ));
        }
        if stream.next_chunk_sequence >= MAX_AGENT_STREAM_CHUNKS as u64 {
            return Err(ProtocolError::Validation(
                "Agent stream has no remaining bounded chunk slot".to_owned(),
            ));
        }
        let staged_content_blocks = stream.staged_content_blocks + chunk.content.len() as u64;
        if staged_content_blocks > MAX_AGENT_VALUE_ENTRIES as u64 {
            return Err(ProtocolError::Validation(format!(
                "Agent stream exceeds {MAX_AGENT_VALUE_ENTRIES} staged content blocks"
            )));
        }
        let content_bytes = cymule_core::canonical_bytes(&chunk.content)?.len() as u64;
        // The final update already includes the array brackets; later chunks add one comma.
        let final_update_bytes = stream.final_update_bytes + content_bytes - 2
            + u64::from(stream.staged_content_blocks != 0);
        if final_update_bytes > MAX_AGENT_VALUE_BYTES as u64 {
            return Err(ProtocolError::Validation(format!(
                "Agent stream final update occupies {final_update_bytes} canonical bytes; maximum is {MAX_AGENT_VALUE_BYTES}"
            )));
        }
        let current = AgentStreamChunkCurrent::new(stream, command_id, chunk)?;
        let staged_bytes = stream
            .staged_bytes
            .checked_add(current.canonical_bytes)
            .ok_or_else(|| {
                ProtocolError::Validation("Agent stream staged byte count is exhausted".to_owned())
            })?;
        if staged_bytes > AGENT_STREAM_STAGING_BYTES_LIMIT as u64 {
            return Err(ProtocolError::Validation(format!(
                "Agent stream exceeds {AGENT_STREAM_STAGING_BYTES_LIMIT} staged bytes"
            )));
        }
        let mut next = stream.clone();
        next.next_chunk_sequence = next.next_chunk_sequence.checked_add(1).ok_or_else(|| {
            ProtocolError::Validation("Agent stream chunk sequence is exhausted".to_owned())
        })?;
        next.chunk_head = Some(current.head.clone());
        next.staged_bytes = staged_bytes;
        next.staged_content_blocks = staged_content_blocks;
        next.final_update_bytes = final_update_bytes;
        command_id.clone_into(&mut next.admitted_by);
        Ok(AgentStreamPostcondition {
            stream: next,
            effect: AgentStreamEffect::Chunk { current },
        })
    }

    fn reduce_abort(
        &self,
        command_id: &str,
        session_id: &str,
        stream_id: &str,
        reason: &str,
    ) -> ProtocolResult<AgentStreamPostcondition> {
        let Self::Abort {
            session,
            stream,
            resource,
            target_claim,
        } = self
        else {
            return Err(stream_source_shape_error());
        };
        session.verify()?;
        stream.verify()?;
        if stream.session_id != session_id
            || stream.stream_id != stream_id
            || stream.state != AgentStreamState::Open
            || session.session_id != session_id
        {
            return Err(ProtocolError::IllegalTransition(
                "Agent stream abort requires its exact open current".to_owned(),
            ));
        }
        let (resource_release_receipt, target_claim_transition) = reduce_stream_abort_reservation(
            command_id,
            stream,
            resource.as_deref(),
            target_claim.as_deref(),
        )?;
        let mut next = stream.clone();
        next.state = AgentStreamState::Aborted;
        next.publication_reservation = None;
        next.abort_reason = Some(reason.to_owned());
        command_id.clone_into(&mut next.admitted_by);
        let session = close_open_stream(session, stream_id, command_id)?;
        let _ = target_claim_transition;
        Ok(AgentStreamPostcondition {
            stream: next,
            effect: AgentStreamEffect::Aborted {
                session,
                resource_release_receipt,
            },
        })
    }

    fn reduce_staged_finalization(
        &self,
        command_id: &str,
        session_id: &str,
        stream_id: &str,
    ) -> ProtocolResult<AgentStreamPostcondition> {
        let Self::Finalize {
            session,
            stream,
            chunks,
            target,
            update,
            resource,
            target_claim,
        } = self
        else {
            return Err(stream_source_shape_error());
        };
        if matches!(
            stream.delivery,
            AgentStreamDelivery::ExternalResource { .. }
        ) {
            return Err(ProtocolError::IllegalTransition(
                "external Agent stream finalization requires the specialized provider authority"
                    .to_owned(),
            ));
        }
        reduce_stream_finalization(&StreamFinalizationInput {
            command_id,
            session_id,
            stream_id,
            publication: None,
            publication_intent: None,
            session,
            stream,
            chunks,
            target_source: target,
            update_source: update.as_ref(),
            resource_source: resource.as_deref(),
            target_claim_source: target_claim.as_deref(),
        })
    }

    /// Reduce an external finalization with the non-Serde publication obtained
    /// from the framework-owned resolver registry.
    ///
    /// # Errors
    ///
    /// Returns an error when the source is not an external finalization, the
    /// product does not match its retained binding, or the derived coupled
    /// Session/Resource postcondition is invalid.
    pub fn reduce_with_publication(
        &self,
        command: &AgentCommand,
        product: &AgentStreamPublicationProduct,
    ) -> ProtocolResult<AgentStreamPostcondition> {
        command.verify()?;
        let AgentCommandAction::Stream(stream_command) = &command.action else {
            return Err(ProtocolError::Validation(
                "Agent stream publication product requires a Stream command".to_owned(),
            ));
        };
        let (
            AgentStreamCommand::Finalize {
                session_id,
                stream_id,
            },
            Self::Finalize {
                session,
                stream,
                chunks,
                target,
                update,
                resource,
                target_claim,
            },
        ) = (stream_command, self)
        else {
            return Err(ProtocolError::Validation(
                "Agent stream publication product is valid only for finalization".to_owned(),
            ));
        };
        if !matches!(
            stream.delivery,
            AgentStreamDelivery::ExternalResource { .. }
        ) {
            return Err(ProtocolError::Validation(
                "staged Agent stream finalization cannot consume a publication product".to_owned(),
            ));
        }
        let reservation = stream.publication_reservation.as_ref().ok_or_else(|| {
            ProtocolError::IllegalTransition(
                "external Agent stream finalization lost its publication reservation".to_owned(),
            )
        })?;
        reservation.verify()?;
        let expected_intent = reservation.intent.clone();
        let expected_pin =
            agent_stream_resource_profile_pin(session_id, stream_id, product.publication())?;
        if reservation.phase != AgentStreamPublicationReservationPhase::DispatchClaimed
            || reservation.resource_pin_receipt.pin != expected_pin.pin
            || product.intent() != &expected_intent
            || product.resource_profile_pin() != &expected_pin
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream publication product changed its preflight owner".to_owned(),
            ));
        }
        let postcondition = reduce_stream_finalization(&StreamFinalizationInput {
            command_id: &command.command_id,
            session_id,
            stream_id,
            publication: Some(product.publication()),
            publication_intent: Some(product.intent()),
            session,
            stream,
            chunks,
            target_source: target,
            update_source: update.as_ref(),
            resource_source: resource.as_deref(),
            target_claim_source: target_claim.as_deref(),
        })?;
        postcondition.verify_for(stream_command)?;
        Ok(postcondition)
    }

    fn verify_postcondition(
        &self,
        command: &AgentCommand,
        postcondition: &AgentStreamPostcondition,
    ) -> ProtocolResult<()> {
        command.verify()?;
        let AgentCommandAction::Stream(stream_command) = &command.action else {
            return Err(ProtocolError::Validation(
                "Agent stream postcondition requires a Stream command".to_owned(),
            ));
        };
        postcondition.verify_for(stream_command)?;
        let expected = match (stream_command, self, &postcondition.effect) {
            (
                AgentStreamCommand::Finalize { .. },
                Self::Finalize { stream, .. },
                AgentStreamEffect::Finalized {
                    publication_record: Some(record),
                    ..
                },
            ) if matches!(
                stream.delivery,
                AgentStreamDelivery::ExternalResource { .. }
            ) =>
            {
                let publication = agent_stream_publication_from_record(
                    stream_command.session_id(),
                    stream_command.stream_id(),
                    record,
                )?;
                let intent = stream
                    .publication_reservation
                    .as_ref()
                    .ok_or_else(|| {
                        ProtocolError::IdentityMismatch(
                            "external Agent receipt source lost its publication reservation"
                                .to_owned(),
                        )
                    })?
                    .intent
                    .clone();
                let product = AgentStreamPublicationProduct::new(intent, publication)?;
                self.reduce_with_publication(command, &product)?
            }
            _ => self.reduce(&command.command_id, stream_command)?,
        };
        if &expected != postcondition {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream receipt is not the exact authorized transition".to_owned(),
            ));
        }
        Ok(())
    }
}

fn reduce_stream_abort_reservation(
    command_id: &str,
    stream: &AgentStreamCurrent,
    resource: Option<&AgentStreamResourceSource>,
    target_claim: Option<&AgentTargetClaimCurrent>,
) -> ProtocolResult<(
    Option<ResourceReleaseReceipt>,
    Option<AgentTargetClaimTransition>,
)> {
    match (
        stream.publication_reservation.as_deref(),
        resource,
        target_claim,
    ) {
        (None, None, None) => Ok((None, None)),
        (Some(reservation), Some(resource), Some(target_claim)) => {
            reduce_reserved_stream_abort(command_id, stream, reservation, resource, target_claim)
        }
        (Some(_), None, _) => Err(ProtocolError::IllegalTransition(
            "Agent stream abort requires its exact reserved Resource source".to_owned(),
        )),
        (None, Some(_), _) | (None, None, Some(_)) => Err(ProtocolError::Validation(
            "Agent stream abort cannot carry Resource or target-claim state without a reservation"
                .to_owned(),
        )),
        (Some(_), Some(_), None) => Err(ProtocolError::IllegalTransition(
            "Agent stream abort requires its exact target reservation".to_owned(),
        )),
    }
}

fn reduce_reserved_stream_abort(
    command_id: &str,
    stream: &AgentStreamCurrent,
    reservation: &AgentStreamPublicationReservation,
    resource: &AgentStreamResourceSource,
    target_claim: &AgentTargetClaimCurrent,
) -> ProtocolResult<(
    Option<ResourceReleaseReceipt>,
    Option<AgentTargetClaimTransition>,
)> {
    reservation.verify()?;
    if reservation.phase != AgentStreamPublicationReservationPhase::NotApplied {
        return Err(ProtocolError::Conflict {
            code: "agent_stream_publication_abort_unresolved".to_owned(),
            message: "Agent stream publication must be durably NotApplied before abort".to_owned(),
        });
    }
    let retention = resource.retention.as_ref().ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Agent stream abort lost its reserved Resource family".to_owned(),
        )
    })?;
    let pin = resource.pin.as_ref().ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Agent stream abort lost its reserved Resource pin".to_owned(),
        )
    })?;
    verify_stream_abort_resource(reservation, retention, pin)?;
    let expected_target = AgentTargetClaimTarget::from_stream_target(&stream.target);
    verify_stream_reserved_target(stream, reservation, target_claim, &expected_target)?;
    let transition = AgentTargetClaimTransition::new(
        &stream.session_id,
        expected_target,
        Some(target_claim),
        AgentTargetClaimPhase::Released {
            stream_id: stream.stream_id.clone(),
            reservation_id: reservation.reservation_id.clone(),
        },
        command_id,
    )?;
    Ok((
        Some(
            reduce_resource_reserved_pin_release_receipt(command_id, retention, pin)
                .map_err(resource_protocol_error)?,
        ),
        Some(transition),
    ))
}

fn verify_stream_abort_resource(
    reservation: &AgentStreamPublicationReservation,
    retention: &ResourceRetentionCurrent,
    pin: &ResourcePinCurrent,
) -> ProtocolResult<()> {
    let origin = crate::resource::ResourceLifecycleReceiptRef::from_agent_publication_reservation(
        reservation.intent.command_id().to_owned(),
        reservation.intent.session_id().to_owned(),
        reservation.intent.stream_id().to_owned(),
        reservation.reservation_id.clone(),
    )
    .map_err(resource_protocol_error)?;
    if reservation.resource_pin_receipt.pin != pin.pin
        || pin.status != crate::resource::ResourcePinStatus::Reserved
        || pin.last_receipt != origin
        || retention.family != pin.pin.subject.family
    {
        return Err(ProtocolError::Conflict {
            code: "agent_stream_publication_abort_reservation_changed".to_owned(),
            message: "Agent stream abort lost its exact publication reservation".to_owned(),
        });
    }
    Ok(())
}

fn verify_stream_reserved_target(
    stream: &AgentStreamCurrent,
    reservation: &AgentStreamPublicationReservation,
    target_claim: &AgentTargetClaimCurrent,
    expected_target: &AgentTargetClaimTarget,
) -> ProtocolResult<()> {
    target_claim.verify()?;
    if target_claim.session_id != stream.session_id
        || target_claim.target != *expected_target
        || target_claim.admitted_by != reservation.intent.command_id()
        || target_claim.phase
            != (AgentTargetClaimPhase::Reserved {
                stream_id: stream.stream_id.clone(),
                reservation_id: reservation.reservation_id.clone(),
            })
    {
        return Err(ProtocolError::Conflict {
            code: "agent_stream_target_claim_changed".to_owned(),
            message: "Agent stream lost its exact target reservation".to_owned(),
        });
    }
    Ok(())
}

fn stream_source_shape_error() -> ProtocolError {
    ProtocolError::Validation(
        "Agent stream before witness does not match its command transition".to_owned(),
    )
}

struct StreamFinalizationInput<'a> {
    command_id: &'a str,
    session_id: &'a str,
    stream_id: &'a str,
    publication: Option<&'a ResourcePublication>,
    publication_intent: Option<&'a AgentStreamPublicationIntent>,
    session: &'a AgentSessionCurrent,
    stream: &'a AgentStreamCurrent,
    chunks: &'a [AgentStreamChunkCurrent],
    target_source: &'a AgentStreamTargetSource,
    update_source: Option<&'a AgentUpdateCurrent>,
    resource_source: Option<&'a AgentStreamResourceSource>,
    target_claim_source: Option<&'a AgentTargetClaimCurrent>,
}

fn reduce_stream_finalization(
    input: &StreamFinalizationInput<'_>,
) -> ProtocolResult<AgentStreamPostcondition> {
    input.session.verify()?;
    input.stream.verify()?;
    if input.session.session_id != input.session_id
        || input.stream.session_id != input.session_id
        || input.stream.stream_id != input.stream_id
        || input.stream.state != AgentStreamState::Open
    {
        return Err(ProtocolError::IllegalTransition(
            "Agent stream finalization requires its exact open Session current".to_owned(),
        ));
    }
    let target_claim_target = AgentTargetClaimTarget::from_stream_target(&input.stream.target);
    verify_stream_finalization_target_claim_source(
        input.stream,
        input.target_claim_source,
        &target_claim_target,
    )?;
    verify_stream_target_source(input.session_id, &input.stream.target, input.target_source)?;
    verify_stream_chunks(input.stream, input.chunks)?;
    let content = stream_finalization_content(input)?;
    let update = stream_finalization_update(
        input.session_id,
        input.stream_id,
        &input.stream.target,
        input.target_source,
        &content,
    )?;
    let update_source = AgentSessionUpdateSource {
        update: input.update_source.cloned(),
        entry: match input.target_source {
            AgentStreamTargetSource::Message { current } => AgentSessionEntrySource::Message {
                current: current.clone(),
            },
            AgentStreamTargetSource::Tool { current } => AgentSessionEntrySource::Tool {
                current: current.clone(),
            },
        },
        target_claims: vec![AgentTargetClaimSource {
            target: AgentTargetClaimTarget::from_stream_target(&input.stream.target),
            current: input.target_claim_source.cloned(),
        }],
    };
    let session = close_open_stream(input.session, input.stream_id, input.command_id)?;
    let mut session_postcondition =
        session.reduce_update(input.command_id, &update, &update_source)?;
    session_postcondition.session.last_transition = Some(AgentSessionTransitionWitness {
        command_id: input.command_id.to_owned(),
        kind: AgentSessionTransitionKind::Stream,
    });
    session_postcondition.session.verify()?;
    let content_digest = agent_stream_final_update_content_digest(&input.stream.target, &update)?;
    let mut next = input.stream.clone();
    next.state = AgentStreamState::Finalized;
    next.publication_reservation = None;
    next.final_update = Some(update);
    next.content_digest = Some(content_digest);
    input.command_id.clone_into(&mut next.admitted_by);
    let (publication_record, resource_pin_receipt) = stream_finalization_resource(input)?;
    Ok(AgentStreamPostcondition {
        stream: next,
        effect: AgentStreamEffect::Finalized {
            session: Box::new(session_postcondition),
            publication_record,
            resource_pin_receipt,
            finalization_coupling_id: agent_stream_finalization_coupling_id(
                input.session_id,
                input.stream_id,
            )?,
        },
    })
}

fn stream_finalization_content(
    input: &StreamFinalizationInput<'_>,
) -> ProtocolResult<Vec<ContentBlock>> {
    if matches!(
        input.stream.delivery,
        AgentStreamDelivery::ExternalResource { .. }
    ) {
        let publication = input.publication.ok_or_else(|| {
            ProtocolError::Validation(
                "external Agent stream finalization requires its authority-produced publication"
                    .to_owned(),
            )
        })?;
        let intent = input.publication_intent.ok_or_else(|| {
            ProtocolError::Validation(
                "external Agent stream finalization requires its framework-derived intent"
                    .to_owned(),
            )
        })?;
        intent.verify()?;
        if intent.command_id() != input.command_id
            || intent.session_id() != input.session_id
            || intent.stream_id() != input.stream_id
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream finalization intent changed its owner".to_owned(),
            ));
        }
        verify_external_stream_publication(intent, publication)?;
        if !input.chunks.is_empty() || input.resource_source.is_none() {
            return Err(ProtocolError::Validation(
                "external Agent stream finalization requires zero chunks and Resource current"
                    .to_owned(),
            ));
        }
        let resource = intent.resource_handle()?;
        return Ok(vec![ContentBlock::ResourceHandle {
            resource: Box::new(resource),
        }]);
    }
    if input.publication.is_some()
        || input.publication_intent.is_some()
        || input.chunks.is_empty()
        || input.resource_source.is_some()
    {
        return Err(ProtocolError::Validation(
            "chunked Agent stream finalization requires chunks and no Resource current".to_owned(),
        ));
    }
    Ok(input
        .chunks
        .iter()
        .flat_map(|entry| entry.chunk.content.iter().cloned())
        .collect())
}

fn stream_finalization_update(
    session_id: &str,
    stream_id: &str,
    target: &AgentStreamTarget,
    target_source: &AgentStreamTargetSource,
    content: &[ContentBlock],
) -> ProtocolResult<AgentUpdate> {
    Ok(match (target, target_source) {
        (
            AgentStreamTarget::Message { message_id, role },
            AgentStreamTargetSource::Message { current: None },
        ) => AgentUpdate::Message {
            update_id: agent_stream_final_update_id(session_id, stream_id)?,
            message: AgentMessage {
                message_id: message_id.clone(),
                role: *role,
                content: content.to_vec(),
            },
        },
        (
            AgentStreamTarget::Tool { tool_call_id },
            AgentStreamTargetSource::Tool {
                current: Some(current),
            },
        ) if current.session_id == session_id
            && current.tool.tool_call_id == *tool_call_id
            && current.tool.status == ToolCallStatus::InProgress =>
        {
            let mut tool = current.tool.clone();
            tool.status = ToolCallStatus::Completed;
            tool.output = Some(content.to_vec());
            AgentUpdate::Tool {
                update_id: agent_stream_final_update_id(session_id, stream_id)?,
                tool,
            }
        }
        _ => {
            return Err(ProtocolError::IllegalTransition(
                "Agent stream final target source changed before finalization".to_owned(),
            ));
        }
    })
}

fn stream_finalization_resource(
    input: &StreamFinalizationInput<'_>,
) -> ProtocolResult<(Option<ResourceCatalogRecord>, Option<ResourcePinReceipt>)> {
    Ok(match (input.publication, input.resource_source) {
        (Some(publication), Some(source)) => {
            let pin =
                agent_stream_resource_profile_pin(input.session_id, input.stream_id, publication)?
                    .pin;
            let reservation = input
                .stream
                .publication_reservation
                .as_ref()
                .ok_or_else(|| {
                    ProtocolError::IllegalTransition(
                        "external Agent finalization lost its durable publication reservation"
                            .to_owned(),
                    )
                })?;
            reservation.verify()?;
            let retained_pin = source.pin.as_ref().ok_or_else(|| {
                ProtocolError::IllegalTransition(
                    "external Agent finalization lost its reserved Resource pin".to_owned(),
                )
            })?;
            let retained = source.retention.as_ref().ok_or_else(|| {
                ProtocolError::IllegalTransition(
                    "external Agent finalization lost its Resource retention obligation".to_owned(),
                )
            })?;
            let reservation_origin =
                crate::resource::ResourceLifecycleReceiptRef::from_agent_publication_reservation(
                    input.command_id.to_owned(),
                    input.session_id.to_owned(),
                    input.stream_id.to_owned(),
                    reservation.reservation_id.clone(),
                )
                .map_err(resource_protocol_error)?;
            if reservation.resource_pin_receipt.pin != pin
                || retained_pin.pin != pin
                || retained_pin.status != crate::resource::ResourcePinStatus::Reserved
                || retained_pin.last_receipt != reservation_origin
                || retained.family != pin.subject.family
            {
                return Err(ProtocolError::Conflict {
                    code: "agent_stream_publication_reservation_changed".to_owned(),
                    message: "Agent stream publication reservation lost its exact Resource pin"
                        .to_owned(),
                });
            }
            (
                Some(agent_stream_publication_catalog_record(
                    input.session_id,
                    input.stream_id,
                    publication,
                )?),
                Some(
                    ResourcePinReceipt::new(
                        input.command_id.to_owned(),
                        pin,
                        retained.active_pin_count,
                    )
                    .map_err(resource_protocol_error)?,
                ),
            )
        }
        (None, None) => (None, None),
        _ => {
            return Err(ProtocolError::Validation(
                "Agent stream Resource source does not match its publication".to_owned(),
            ));
        }
    })
}

fn preflight_external_stream_publication(
    source: &AgentStreamSource,
    command: &AgentStreamCommand,
    source_revision: &str,
    command_id: &str,
) -> ProtocolResult<AgentStreamPublicationIntent> {
    command.verify()?;
    let (
        AgentStreamCommand::Finalize {
            session_id,
            stream_id,
        },
        AgentStreamSource::Finalize {
            session,
            stream:
                stream @ AgentStreamCurrent {
                    delivery:
                        AgentStreamDelivery::ExternalResource {
                            resolver_binding,
                            content,
                        },
                    ..
                },
            chunks,
            target,
            update,
            resource: _,
            target_claim: _,
        },
    ) = (command, source)
    else {
        return Err(ProtocolError::Validation(
            "Agent stream provider accepts only an external Finalize source".to_owned(),
        ));
    };
    session.verify()?;
    stream.verify()?;
    if session.session_id != *session_id
        || stream.session_id != *session_id
        || stream.stream_id != *stream_id
        || stream.state != AgentStreamState::Open
        || stream.publication_reservation.is_some()
        || !chunks.is_empty()
        || update.is_some()
    {
        return Err(ProtocolError::IllegalTransition(
            "external Agent stream provider preflight rejected its exact current".to_owned(),
        ));
    }
    verify_stream_target_source(session_id, &stream.target, target)?;
    verify_stream_chunks(stream, chunks)?;
    close_open_stream(session, stream_id, command_id)?;
    AgentStreamPublicationIntent::new(
        source_revision,
        source,
        command_id,
        stream,
        resolver_binding,
        content,
    )
}

/// Derive the immutable external publication intent and its exact physical
/// retention selector before provider I/O.
///
/// # Errors
///
/// Returns an error when the command/source is not one admissible external
/// finalization or cannot identify one exact profile pin.
pub fn prepare_agent_stream_publication(
    source: &AgentStreamSource,
    command: &AgentCommand,
) -> ProtocolResult<(AgentStreamPublicationIntent, ResourceProfilePin)> {
    command.verify()?;
    let AgentCommandAction::Stream(stream_command) = &command.action else {
        return Err(ProtocolError::Validation(
            "Agent stream publication preparation requires a Stream command".to_owned(),
        ));
    };
    let intent = preflight_external_stream_publication(
        source,
        stream_command,
        &command.source_revision,
        &command.command_id,
    )?;
    let pin = agent_stream_resource_profile_pin_from_intent(&intent)?;
    Ok((intent, pin))
}

/// Persistable postcondition for the first pre-publication retention CAS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamPublicationReservationPostcondition {
    /// Open stream current carrying the newly claimed dispatch attempt.
    pub stream: AgentStreamCurrent,
    /// Exact reserved Resource pin receipt used by Durable lowering.
    pub resource_pin_receipt: ResourcePinReceipt,
    /// Exact independent target claim acquired before provider I/O.
    pub target_claim: AgentTargetClaimTransition,
}

/// Reduce one external publication reservation over its exact Agent and
/// Resource source. This pure transition performs no provider I/O.
///
/// # Errors
///
/// Returns an error when the stream already has a reservation, a deletion
/// fence owns the physical family, or the exact pin source changed.
pub fn reserve_agent_stream_publication(
    source: &AgentStreamSource,
    command: &AgentCommand,
) -> ProtocolResult<AgentStreamPublicationReservationPostcondition> {
    let (intent, profile_pin) = prepare_agent_stream_publication(source, command)?;
    let AgentStreamSource::Finalize {
        stream,
        resource: Some(resource),
        target_claim,
        ..
    } = source
    else {
        return Err(ProtocolError::Validation(
            "Agent publication reservation requires its exact Resource source".to_owned(),
        ));
    };
    if stream.publication_reservation.is_some() {
        return Err(ProtocolError::Conflict {
            code: "agent_stream_publication_already_reserved".to_owned(),
            message: "Agent stream already retains a publication reservation".to_owned(),
        });
    }
    let resource_pin_receipt = reduce_resource_pin_receipt(
        &command.command_id,
        &profile_pin.pin,
        resource.retention.as_ref(),
        resource.pin.as_ref(),
    )
    .map_err(resource_protocol_error)?;
    let reservation = AgentStreamPublicationReservation::new(intent, resource_pin_receipt.clone())?;
    let target = AgentTargetClaimTarget::from_stream_target(&stream.target);
    let target_source = AgentTargetClaimSource {
        target: target.clone(),
        current: target_claim.as_deref().cloned(),
    };
    target_source.verify_for(&stream.session_id)?;
    require_unclaimed_target(&target_source)?;
    let target_claim = AgentTargetClaimTransition::new(
        &stream.session_id,
        target,
        target_source.current.as_ref(),
        AgentTargetClaimPhase::Reserved {
            stream_id: stream.stream_id.clone(),
            reservation_id: reservation.reservation_id.clone(),
        },
        &command.command_id,
    )?;
    let mut next = stream.clone();
    next.publication_reservation = Some(Box::new(reservation));
    next.verify()?;
    Ok(AgentStreamPublicationReservationPostcondition {
        stream: next,
        resource_pin_receipt,
        target_claim,
    })
}

/// Rearm one provider-proved `NotApplied` reservation. Only the fresh CAS that
/// publishes this successor may invoke the provider again.
///
/// # Errors
///
/// Returns an error when the stream/command owner changed or the current
/// attempt has not reached `NotApplied`.
pub fn rearm_agent_stream_publication(
    stream: &AgentStreamCurrent,
    command: &AgentCommand,
) -> ProtocolResult<AgentStreamCurrent> {
    stream.verify()?;
    command.verify()?;
    let AgentCommandAction::Stream(AgentStreamCommand::Finalize {
        session_id,
        stream_id,
    }) = &command.action
    else {
        return Err(ProtocolError::Validation(
            "Agent publication rearm requires an external Finalize command".to_owned(),
        ));
    };
    let reservation = stream.publication_reservation.as_ref().ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Agent publication rearm requires its durable reservation".to_owned(),
        )
    })?;
    if stream.session_id != *session_id
        || stream.stream_id != *stream_id
        || reservation.intent.command_id() != command.command_id
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent publication rearm changed its exact stream command".to_owned(),
        ));
    }
    let mut next = stream.clone();
    next.publication_reservation = Some(Box::new(reservation.rearm()?));
    next.verify()?;
    Ok(next)
}

/// Retain exact `NotApplied` observation for the latest claimed provider attempt.
///
/// # Errors
///
/// Returns an error when the stream/command owner changed or no attempt is
/// currently dispatch-claimed.
pub fn mark_agent_stream_publication_not_applied(
    stream: &AgentStreamCurrent,
    command: &AgentCommand,
) -> ProtocolResult<AgentStreamCurrent> {
    stream.verify()?;
    command.verify()?;
    let reservation = stream.publication_reservation.as_ref().ok_or_else(|| {
        ProtocolError::IllegalTransition(
            "Agent publication observation requires its durable reservation".to_owned(),
        )
    })?;
    if reservation.intent.command_id() != command.command_id {
        return Err(ProtocolError::IdentityMismatch(
            "Agent publication observation changed its exact Finalize command".to_owned(),
        ));
    }
    let mut next = stream.clone();
    next.publication_reservation = Some(Box::new(reservation.mark_not_applied()?));
    next.verify()?;
    Ok(next)
}

/// Derive the exact independent target-claim mutation owned by one terminal
/// stream receipt.
///
/// # Errors
///
/// Returns an error when the command, source reservation, target, or terminal
/// postcondition does not close one legal claim successor.
pub fn agent_stream_target_claim_transition(
    command: &AgentCommand,
    source: &AgentStreamSource,
    postcondition: &AgentStreamPostcondition,
) -> ProtocolResult<Option<AgentTargetClaimTransition>> {
    source.verify_postcondition(command, postcondition)?;
    let AgentCommandAction::Stream(stream_command) = &command.action else {
        return Err(ProtocolError::Validation(
            "Agent stream target claim requires a Stream command".to_owned(),
        ));
    };
    match (stream_command, source, &postcondition.effect) {
        (
            AgentStreamCommand::Finalize {
                session_id,
                stream_id,
            },
            AgentStreamSource::Finalize {
                stream,
                target_claim,
                ..
            },
            AgentStreamEffect::Finalized { session, .. },
        ) => Ok(Some(finalized_stream_target_claim_transition(
            &command.command_id,
            session_id,
            stream_id,
            stream,
            target_claim.as_deref(),
            session,
        )?)),
        (
            AgentStreamCommand::Abort {
                session_id,
                stream_id,
                ..
            },
            AgentStreamSource::Abort {
                stream,
                target_claim,
                ..
            },
            AgentStreamEffect::Aborted {
                resource_release_receipt,
                ..
            },
        ) => aborted_stream_target_claim_transition(
            &command.command_id,
            session_id,
            stream_id,
            stream,
            target_claim.as_deref(),
            resource_release_receipt.as_ref(),
        ),
        (
            AgentStreamCommand::Open { .. } | AgentStreamCommand::AppendChunk { .. },
            AgentStreamSource::Open { .. } | AgentStreamSource::AppendChunk { .. },
            AgentStreamEffect::Opened { .. } | AgentStreamEffect::Chunk { .. },
        ) => Ok(None),
        _ => Err(ProtocolError::IdentityMismatch(
            "Agent stream target claim does not match its command receipt".to_owned(),
        )),
    }
}

fn finalized_stream_target_claim_transition(
    command_id: &str,
    session_id: &str,
    stream_id: &str,
    stream: &AgentStreamCurrent,
    target_claim: Option<&AgentTargetClaimCurrent>,
    session: &AgentSessionPostcondition,
) -> ProtocolResult<AgentTargetClaimTransition> {
    if stream.session_id != session_id
        || stream.stream_id != stream_id
        || session.session.session_id != session_id
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent stream finalization target claim changed its owner".to_owned(),
        ));
    }
    let target = AgentTargetClaimTarget::from_stream_target(&stream.target);
    let claim_source = AgentTargetClaimSource {
        target: target.clone(),
        current: target_claim.cloned(),
    };
    claim_source.verify_for(session_id)?;
    verify_stream_finalization_target_claim_source(stream, target_claim, &target)?;
    materialize_target_claim(session_id, target, &claim_source, command_id)
}

fn verify_stream_finalization_target_claim_source(
    stream: &AgentStreamCurrent,
    target_claim: Option<&AgentTargetClaimCurrent>,
    target: &AgentTargetClaimTarget,
) -> ProtocolResult<()> {
    match (stream.publication_reservation.as_deref(), target_claim) {
        (Some(reservation), Some(current)) => {
            verify_stream_reserved_target(stream, reservation, current, target)?;
        }
        (Some(_), None) => {
            return Err(ProtocolError::IdentityMismatch(
                "Agent external finalization lost its target reservation".to_owned(),
            ));
        }
        (None, Some(current))
            if matches!(current.phase, AgentTargetClaimPhase::Reserved { .. }) =>
        {
            return Err(ProtocolError::Conflict {
                code: "agent_target_already_claimed".to_owned(),
                message: "Agent staged finalization cannot consume an external reservation"
                    .to_owned(),
            });
        }
        (None, _) => {}
    }
    Ok(())
}

fn aborted_stream_target_claim_transition(
    command_id: &str,
    session_id: &str,
    stream_id: &str,
    stream: &AgentStreamCurrent,
    target_claim: Option<&AgentTargetClaimCurrent>,
    resource_release_receipt: Option<&ResourceReleaseReceipt>,
) -> ProtocolResult<Option<AgentTargetClaimTransition>> {
    match (
        stream.publication_reservation.as_deref(),
        target_claim,
        resource_release_receipt,
    ) {
        (None, None, None) => Ok(None),
        (Some(reservation), Some(source), Some(_)) => {
            reservation.verify()?;
            let target = AgentTargetClaimTarget::from_stream_target(&stream.target);
            if stream.session_id != session_id
                || stream.stream_id != stream_id
                || source.session_id != session_id
                || source.target != target
                || source.admitted_by != reservation.intent.command_id()
                || source.phase
                    != (AgentTargetClaimPhase::Reserved {
                        stream_id: stream_id.to_owned(),
                        reservation_id: reservation.reservation_id.clone(),
                    })
            {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent stream Abort changed its target reservation".to_owned(),
                ));
            }
            Ok(Some(AgentTargetClaimTransition::new(
                session_id,
                target,
                Some(source),
                AgentTargetClaimPhase::Released {
                    stream_id: stream_id.to_owned(),
                    reservation_id: reservation.reservation_id.clone(),
                },
                command_id,
            )?))
        }
        _ => Err(ProtocolError::IdentityMismatch(
            "Agent stream Abort retained a partial target-claim transition".to_owned(),
        )),
    }
}

fn agent_stream_publication_source_digest(source: &AgentStreamSource) -> ProtocolResult<String> {
    let mut preflight = source.clone();
    let AgentStreamSource::Finalize { resource, .. } = &mut preflight else {
        return Err(ProtocolError::Validation(
            "Agent stream publication source digest requires Finalize".to_owned(),
        ));
    };
    *resource = None;
    canonical_digest(&preflight).map_err(Into::into)
}

fn verify_external_stream_publication(
    intent: &AgentStreamPublicationIntent,
    publication: &ResourcePublication,
) -> ProtocolResult<()> {
    intent.verify()?;
    verify_external_stream_publication_binding(
        intent.resolver_binding(),
        intent.content(),
        publication,
    )
}

fn verify_external_stream_publication_binding(
    resolver_binding: &str,
    content: &AgentStreamPublicationContent,
    publication: &ResourcePublication,
) -> ProtocolResult<()> {
    publication.verify().map_err(resource_protocol_error)?;
    let expected_resource = external_stream_resource_handle(content)?;
    if publication.locators.resolver_binding != resolver_binding
        || publication.resource != expected_resource
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent external stream publication changed its resolver binding or semantic Resource"
                .to_owned(),
        ));
    }
    Ok(())
}

fn agent_stream_resource_profile_pin(
    session_id: &str,
    stream_id: &str,
    publication: &ResourcePublication,
) -> ProtocolResult<ResourceProfilePin> {
    publication.verify().map_err(resource_protocol_error)?;
    let intent_resource = &publication.resource;
    let subject = ResourceRetentionSubject::from_handle(
        &publication.locators.resolver_binding,
        intent_resource,
    )
    .map_err(resource_protocol_error)?;
    let pin = ResourcePin::profile(
        subject,
        ResourcePinKind::AgentStream {
            session_id: session_id.to_owned(),
            stream_id: stream_id.to_owned(),
        },
    )
    .map_err(resource_protocol_error)?;
    ResourceProfilePin::new(pin).map_err(resource_protocol_error)
}

fn agent_stream_resource_profile_pin_from_intent(
    intent: &AgentStreamPublicationIntent,
) -> ProtocolResult<ResourceProfilePin> {
    intent.verify()?;
    let resource = intent.resource_handle()?;
    let subject = ResourceRetentionSubject::from_handle(intent.resolver_binding(), &resource)
        .map_err(resource_protocol_error)?;
    let pin = ResourcePin::profile(
        subject,
        ResourcePinKind::AgentStream {
            session_id: intent.session_id().to_owned(),
            stream_id: intent.stream_id().to_owned(),
        },
    )
    .map_err(resource_protocol_error)?;
    ResourceProfilePin::new(pin).map_err(resource_protocol_error)
}

fn agent_stream_publication_catalog_record(
    session_id: &str,
    stream_id: &str,
    publication: &ResourcePublication,
) -> ProtocolResult<ResourceCatalogRecord> {
    let key = agent_stream_publication_key(session_id, stream_id)?;
    let payload = cymule_core::canonical_bytes(publication)?;
    ResourceCatalogRecord::new(AGENT_STREAM_PUBLICATION_NAMESPACE, key, payload)
        .map_err(resource_protocol_error)
}

fn agent_stream_publication_from_record(
    session_id: &str,
    stream_id: &str,
    record: &ResourceCatalogRecord,
) -> ProtocolResult<ResourcePublication> {
    record.verify().map_err(resource_protocol_error)?;
    if record.namespace != AGENT_STREAM_PUBLICATION_NAMESPACE
        || record.key != agent_stream_publication_key(session_id, stream_id)?
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent stream publication record changed its exact catalog owner".to_owned(),
        ));
    }
    let publication: ResourcePublication = cymule_core::decode_json(&record.payload)?;
    publication.verify().map_err(resource_protocol_error)?;
    Ok(publication)
}

fn verify_stream_target_source(
    session_id: &str,
    target: &AgentStreamTarget,
    source: &AgentStreamTargetSource,
) -> ProtocolResult<()> {
    match (target, source) {
        (
            AgentStreamTarget::Message { message_id, .. },
            AgentStreamTargetSource::Message { current: None },
        ) => {
            validate_identity("Agent stream message target", message_id)?;
            Ok(())
        }
        (
            AgentStreamTarget::Message { .. },
            AgentStreamTargetSource::Message { current: Some(_) },
        ) => Err(ProtocolError::IllegalTransition(
            "Agent stream message target must not already have an immutable alias".to_owned(),
        )),
        (
            AgentStreamTarget::Tool { tool_call_id },
            AgentStreamTargetSource::Tool {
                current: Some(current),
            },
        ) => {
            current.verify()?;
            if current.session_id != session_id
                || current.tool.tool_call_id != *tool_call_id
                || current.tool.status != ToolCallStatus::InProgress
            {
                return Err(ProtocolError::IllegalTransition(
                    "Agent stream tool target must be the exact in-progress tool current"
                        .to_owned(),
                ));
            }
            Ok(())
        }
        _ => Err(ProtocolError::Validation(
            "Agent stream target source has the wrong typed shape".to_owned(),
        )),
    }
}

fn verify_stream_chunks(
    stream: &AgentStreamCurrent,
    chunks: &[AgentStreamChunkCurrent],
) -> ProtocolResult<()> {
    if chunks.len()
        != usize::try_from(stream.next_chunk_sequence)
            .expect("verified Agent stream chunk count fits usize")
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent stream finalize source does not contain every staged chunk".to_owned(),
        ));
    }
    let mut previous_head: Option<&str> = None;
    let mut staged_bytes = 0_u64;
    let mut staged_content_blocks = 0_u64;
    for (sequence, chunk) in chunks.iter().enumerate() {
        chunk.verify()?;
        if chunk.session_id != stream.session_id
            || chunk.stream_id != stream.stream_id
            || chunk.chunk.sequence != sequence as u64
            || chunk.previous_head.as_deref() != previous_head
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream finalize source changed its chunk order".to_owned(),
            ));
        }
        staged_bytes = staged_bytes
            .checked_add(chunk.canonical_bytes)
            .ok_or_else(|| {
                ProtocolError::Validation("Agent stream staged byte count is exhausted".to_owned())
            })?;
        staged_content_blocks += chunk.chunk.content.len() as u64;
        previous_head = Some(&chunk.head);
    }
    if previous_head != stream.chunk_head.as_deref()
        || staged_bytes != stream.staged_bytes
        || staged_content_blocks != stream.staged_content_blocks
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent stream finalize source does not match its bounded current".to_owned(),
        ));
    }
    Ok(())
}

impl AgentStreamPostcondition {
    /// Verify the bounded exact postcondition shape for one stream command.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream, affected Session entry, publication,
    /// Resource pin, or finalization coupling does not match the command.
    pub fn verify_for(&self, command: &AgentStreamCommand) -> ProtocolResult<()> {
        self.stream.verify()?;
        if self.stream.session_id != command.session_id()
            || self.stream.stream_id != command.stream_id()
        {
            return Err(ProtocolError::IdentityMismatch(
                "Agent stream postcondition changed its owner".to_owned(),
            ));
        }
        match (command, &self.effect) {
            (
                AgentStreamCommand::Open {
                    target, delivery, ..
                },
                AgentStreamEffect::Opened { session },
            ) if self.stream.state == AgentStreamState::Open
                && self.stream.target == *target
                && self.stream.delivery == *delivery
                && self.stream.next_chunk_sequence == 0 =>
            {
                verify_stream_session_effect(session, &self.stream)
            }
            (
                AgentStreamCommand::AppendChunk { chunk, .. },
                AgentStreamEffect::Chunk { current },
            ) if self.stream.state == AgentStreamState::Open
                && current.chunk == *chunk
                && self.stream.chunk_head.as_deref() == Some(current.head.as_str()) =>
            {
                current.verify()
            }
            (
                AgentStreamCommand::Abort { reason, .. },
                AgentStreamEffect::Aborted {
                    session,
                    resource_release_receipt,
                },
            ) if self.stream.state == AgentStreamState::Aborted
                && self.stream.abort_reason.as_deref() == Some(reason) =>
            {
                verify_stream_session_effect(session, &self.stream)?;
                if let Some(receipt) = resource_release_receipt {
                    receipt.verify().map_err(resource_protocol_error)?;
                    if !matches!(
                        &receipt.pin.kind,
                        ResourcePinKind::AgentStream {
                            session_id,
                            stream_id,
                        } if session_id == &self.stream.session_id
                            && stream_id == &self.stream.stream_id
                    ) || !matches!(
                        self.stream.delivery,
                        AgentStreamDelivery::ExternalResource { .. }
                    ) {
                        return Err(ProtocolError::IdentityMismatch(
                            "Agent stream abort Resource release changed its exact owner"
                                .to_owned(),
                        ));
                    }
                }
                Ok(())
            }
            (
                AgentStreamCommand::Finalize {
                    session_id,
                    stream_id,
                },
                AgentStreamEffect::Finalized {
                    session,
                    publication_record,
                    resource_pin_receipt,
                    finalization_coupling_id,
                },
            ) if self.stream.state == AgentStreamState::Finalized => {
                verify_stream_finalization_effect(
                    &self.stream,
                    session_id,
                    stream_id,
                    session,
                    publication_record.as_ref(),
                    resource_pin_receipt.as_ref(),
                    finalization_coupling_id,
                )
            }
            _ => Err(ProtocolError::IdentityMismatch(
                "Agent stream postcondition does not match its exact command".to_owned(),
            )),
        }
    }
}

fn verify_stream_finalization_effect(
    stream: &AgentStreamCurrent,
    session_id: &str,
    stream_id: &str,
    session: &AgentSessionPostcondition,
    publication_record: Option<&ResourceCatalogRecord>,
    resource_pin_receipt: Option<&ResourcePinReceipt>,
    finalization_coupling_id: &str,
) -> ProtocolResult<()> {
    let update = stream.final_update.as_ref().ok_or_else(|| {
        ProtocolError::IdentityMismatch(
            "Agent stream finalization lost its Session update".to_owned(),
        )
    })?;
    session.verify_for(update)?;
    if finalization_coupling_id != agent_stream_finalization_coupling_id(session_id, stream_id)? {
        return Err(ProtocolError::IdentityMismatch(
            "Agent stream finalization changed its coupling".to_owned(),
        ));
    }
    match (&stream.delivery, publication_record, resource_pin_receipt) {
        (
            AgentStreamDelivery::ExternalResource {
                resolver_binding,
                content,
            },
            Some(record),
            Some(receipt),
        ) => {
            receipt.verify().map_err(resource_protocol_error)?;
            let publication = agent_stream_publication_from_record(session_id, stream_id, record)?;
            verify_external_stream_publication_binding(resolver_binding, content, &publication)?;
            let expected = agent_stream_resource_profile_pin(session_id, stream_id, &publication)?;
            if receipt.pin != expected.pin {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent stream finalization retained a different Resource pin".to_owned(),
                ));
            }
            Ok(())
        }
        (AgentStreamDelivery::Staged, None, None) => Ok(()),
        _ => Err(ProtocolError::IdentityMismatch(
            "Agent stream finalization Resource receipt has the wrong shape".to_owned(),
        )),
    }
}

fn verify_stream_session_effect(
    session: &AgentSessionCurrent,
    stream: &AgentStreamCurrent,
) -> ProtocolResult<()> {
    session.verify()?;
    if session.session_id != stream.session_id
        || session.last_transition.as_ref()
            != Some(&AgentSessionTransitionWitness {
                command_id: stream.admitted_by.clone(),
                kind: AgentSessionTransitionKind::Stream,
            })
    {
        return Err(ProtocolError::IdentityMismatch(
            "Agent stream Session effect changed its owner or command witness".to_owned(),
        ));
    }
    Ok(())
}

/// Closed Plan-owned input command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "transition", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentInputCommand {
    /// Attach a Session elicitation to one already-pending input Wait.
    Suspend {
        /// Owning Session identity.
        session_id: String,
        /// Exact Plan-owned Wait identity.
        wait_id: String,
        /// Expected owning Run.
        expected_run_id: String,
        /// Complete structural Wait owner.
        expected_owner: WaitOwner,
        /// Exact typed elicitation request.
        request: ElicitationRequest,
    },
    /// Resolve the exact suspension and Session response in one CAS.
    Complete {
        /// Owning Session identity.
        session_id: String,
        /// Exact Plan-owned Wait identity.
        wait_id: String,
        /// Expected owning Run.
        expected_run_id: String,
        /// Complete structural Wait owner.
        expected_owner: WaitOwner,
        /// Exact typed Session response.
        response: ElicitationResponse,
    },
}

impl AgentInputCommand {
    /// Owning Session identity.
    pub fn session_id(&self) -> &str {
        match self {
            Self::Suspend { session_id, .. } | Self::Complete { session_id, .. } => session_id,
        }
    }

    /// Exact Plan-owned Wait identity.
    pub fn wait_id(&self) -> &str {
        match self {
            Self::Suspend { wait_id, .. } | Self::Complete { wait_id, .. } => wait_id,
        }
    }

    /// Verify structural ownership and request/response-local invariants.
    ///
    /// # Errors
    ///
    /// Returns an error when a Session, Wait, Run, structural owner, request, or
    /// response is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        validate_identity("Agent Session", self.session_id())?;
        validate_identity("Agent input Wait", self.wait_id())?;
        match self {
            Self::Suspend {
                expected_run_id,
                expected_owner,
                request,
                ..
            } => {
                validate_identity("Agent input Run", expected_run_id)?;
                expected_owner.verify()?;
                validate_elicitation_request(request)
            }
            Self::Complete {
                expected_run_id,
                expected_owner,
                response,
                ..
            } => {
                validate_identity("Agent input Run", expected_run_id)?;
                expected_owner.verify()?;
                response.validate()
            }
        }
    }
}

/// Closed workspace effect settlement result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentWorkspaceResolution {
    /// Provider evidence proves the commit applied.
    Applied,
    /// Provider evidence proves the commit did not apply.
    NotApplied,
    /// The original commit remains ambiguous.
    Unknown,
}

/// Closed workspace scope/effect command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "transition", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentWorkspaceCommand {
    /// Atomically start the admitted mutating Effect and its host occurrence.
    StartEffect {
        /// Complete semantic workspace owner.
        request: WorkspaceScopeRequest,
        /// Runtime-derived structural Effect intent.
        effect_intent_id: String,
        /// Retained exact execution-binding Artifact.
        execution_binding: ArtifactRef,
        /// Runtime-derived operation occurrence binding.
        operation_occurrence_binding: String,
    },
    /// Atomically settle the original Effect, outbox, obligation, and occurrence.
    SettleEffect {
        /// Complete semantic workspace owner.
        request: WorkspaceScopeRequest,
    },
    /// Atomically retain the abort occurrence as Started before provider dispatch.
    StartAbort {
        /// Complete semantic workspace owner.
        request: WorkspaceScopeRequest,
        /// Retained exact execution-binding Artifact.
        execution_binding: ArtifactRef,
        /// Runtime-derived operation occurrence binding.
        operation_occurrence_binding: String,
    },
    /// Atomically settle the original abort observation and, only when applied,
    /// close the owning scope.
    SettleAbort {
        /// Complete semantic workspace owner.
        request: WorkspaceScopeRequest,
    },
}

impl AgentWorkspaceCommand {
    /// Complete semantic owner carried by the command.
    pub const fn request(&self) -> &WorkspaceScopeRequest {
        match self {
            Self::StartEffect { request, .. }
            | Self::SettleEffect { request, .. }
            | Self::StartAbort { request, .. }
            | Self::SettleAbort { request, .. } => request,
        }
    }

    /// Derive the exact M1 checkpoint phase for one verified workspace result.
    ///
    /// # Errors
    ///
    /// Returns an error when the command or occurrence cannot own a closed
    /// workspace checkpoint phase.
    pub fn phase_for(
        &self,
        occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<AgentWorkspaceCommandPhase> {
        self.verify()?;
        verify_workspace_occurrence_owner(
            occurrence,
            self.request(),
            matches!(self, Self::StartEffect { .. } | Self::SettleEffect { .. }),
        )?;
        workspace_checkpoint_phase(self, occurrence)
    }

    /// Verify the complete command-local workspace intent.
    ///
    /// # Errors
    ///
    /// Returns an error when the semantic owner, Effect intent, execution
    /// binding, or operation occurrence binding is invalid.
    pub fn verify(&self) -> ProtocolResult<()> {
        let request = self.request();
        request.verify()?;
        match self {
            Self::StartEffect {
                request,
                effect_intent_id,
                execution_binding,
                operation_occurrence_binding,
                ..
            } => {
                request.dispatch_lease.as_ref().ok_or_else(|| {
                    ProtocolError::Validation(
                        "workspace Effect start requires its exact dispatch lease request"
                            .to_owned(),
                    )
                })?;
                validate_identity("workspace Effect intent", effect_intent_id)?;
                execution_binding.validate()?;
                if execution_binding.kind != EXECUTION_BINDING_ARTIFACT_KIND {
                    return Err(ProtocolError::Validation(
                        "workspace Effect start requires an ExecutionBinding Artifact".to_owned(),
                    ));
                }
                validate_identity(
                    "workspace operation occurrence binding",
                    operation_occurrence_binding,
                )
                .map_err(Into::into)
            }
            Self::StartAbort {
                request,
                execution_binding,
                operation_occurrence_binding,
                ..
            } => {
                if request.dispatch_lease.is_some() {
                    return Err(ProtocolError::Validation(
                        "workspace abort start cannot carry an Effect dispatch lease".to_owned(),
                    ));
                }
                execution_binding.validate()?;
                if execution_binding.kind != EXECUTION_BINDING_ARTIFACT_KIND {
                    return Err(ProtocolError::Validation(
                        "workspace abort start requires an ExecutionBinding Artifact".to_owned(),
                    ));
                }
                validate_identity(
                    "workspace operation occurrence binding",
                    operation_occurrence_binding,
                )
                .map_err(Into::into)
            }
            Self::SettleEffect { request } | Self::SettleAbort { request } => {
                if request.dispatch_lease.is_some() {
                    return Err(ProtocolError::Validation(
                        "workspace settlement cannot carry a new dispatch lease".to_owned(),
                    ));
                }
                Ok(())
            }
        }
    }
}

/// Shape-matched durable result for one closed Agent command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCommandReceipt {
    /// Frozen receipt generation.
    pub receipt_version: String,
    /// Self-authenticating identity of the command and exact typed outcome.
    pub receipt_id: String,
    /// Exact admitted command identity.
    pub command_id: String,
    /// Bounded exact before witness resolved at the command source revision.
    pub source: AgentCommandSource,
    /// Typed bounded post-commit projection.
    pub outcome: AgentCommandOutcome,
}

/// Non-persisted physical commit envelope returned by the Durable façade.
///
/// `observed_revision` is deliberately outside [`AgentCommandReceipt`]: the
/// receipt is itself stored in `StateRoot`, so embedding that root identity in
/// the receipt would require an impossible content-addressed fixed point. On a
/// first commit it is the resulting head; on a later idempotent replay it is
/// the current head observed while returning the stable semantic receipt.
/// `committed_revision` separately identifies this call's acknowledged write;
/// exact replay carries null even when it observes that same resulting head.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCommit {
    /// Exact physical `StateRoot` revision observed by this façade call.
    pub observed_revision: String,
    /// Resulting revision only when this call freshly committed its command.
    /// Exact receipt replay always carries explicit null, even at the same head.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub committed_revision: Option<String>,
    /// Self-contained stable semantic command receipt.
    pub receipt: AgentCommandReceipt,
}

impl AgentCommit {
    /// Verify the observed revision, fresh acknowledgement, and semantic receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when either revision is malformed, the fresh
    /// acknowledgement differs from the observed revision, or the stable
    /// semantic receipt does not match the command.
    pub fn verify_for(&self, command: &AgentCommand) -> ProtocolResult<()> {
        validate_sha256("Agent commit observed revision", &self.observed_revision)?;
        self.receipt.verify_for(command)?;
        if let Some(committed) = &self.committed_revision {
            validate_sha256("Agent commit committed revision", committed)?;
            if committed != &self.observed_revision {
                return Err(ProtocolError::IdentityMismatch(
                    "Agent commit acknowledgement does not match its observed revision".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// Closed result of one staged or external stream finalization attempt.
///
/// A provider publication may have applied even when the later Durable CAS did
/// not return a commit receipt. The Unknown variant retains the exact immutable
/// publication intent required for provider-ledger reconciliation.
#[derive(Debug, Clone, PartialEq)]
pub enum AgentStreamFinalizeOutcome {
    /// The Agent/Resource postcondition committed or replayed exactly.
    Committed {
        /// Stable semantic Agent commit.
        commit: Box<AgentCommit>,
    },
    /// Provider evidence proves that the publication intent did not apply.
    PublicationNotApplied {
        /// Exact durable `NotApplied` reservation generation.
        dispatch: Box<AgentStreamPublicationReservation>,
        /// Exact intent which remains safe to invoke idempotently.
        intent: AgentStreamPublicationIntent,
    },
    /// Publication or post-publication CAS outcome remains ambiguous.
    PublicationOutcomeUnknown {
        /// Exact intent required for provider-ledger reconciliation.
        intent: AgentStreamPublicationIntent,
    },
}

impl AgentStreamFinalizeOutcome {
    /// Borrow the committed Agent receipt when this attempt reached Durable authority.
    #[must_use]
    pub const fn commit(&self) -> Option<&AgentCommit> {
        match self {
            Self::Committed { commit } => Some(commit),
            Self::PublicationNotApplied { .. } | Self::PublicationOutcomeUnknown { .. } => None,
        }
    }

    /// Borrow the unresolved publication intent, when provider reconciliation remains.
    #[must_use]
    pub const fn publication_intent(&self) -> Option<&AgentStreamPublicationIntent> {
        match self {
            Self::Committed { .. } | Self::PublicationNotApplied { .. } => None,
            Self::PublicationOutcomeUnknown { intent } => Some(intent),
        }
    }

    /// Verify this outcome against the exact Finalize command.
    ///
    /// # Errors
    ///
    /// Returns an error when the commit or publication intent belongs to
    /// another command, Session, stream, or source revision.
    pub fn verify_for(&self, command: &AgentCommand) -> ProtocolResult<()> {
        command.verify()?;
        let AgentCommandAction::Stream(AgentStreamCommand::Finalize {
            session_id,
            stream_id,
        }) = &command.action
        else {
            return Err(ProtocolError::Validation(
                "Agent stream outcome requires a Finalize command".to_owned(),
            ));
        };
        match self {
            Self::Committed { commit } => commit.verify_for(command),
            Self::PublicationNotApplied { dispatch, intent } => {
                dispatch.verify()?;
                intent.verify()?;
                if dispatch.phase != AgentStreamPublicationReservationPhase::NotApplied
                    || dispatch.intent != *intent
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent stream NotApplied outcome changed its durable dispatch proof"
                            .to_owned(),
                    ));
                }
                if intent.source_revision() != command.source_revision
                    || intent.command_id() != command.command_id
                    || intent.session_id() != session_id
                    || intent.stream_id() != stream_id
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent stream outcome changed its exact Finalize owner".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::PublicationOutcomeUnknown { intent } => {
                intent.verify()?;
                if intent.source_revision() != command.source_revision
                    || intent.command_id() != command.command_id
                    || intent.session_id() != session_id
                    || intent.stream_id() != stream_id
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent stream outcome changed its exact Finalize owner".to_owned(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Verify this reconciliation result against the exact restored intent.
    ///
    /// # Errors
    ///
    /// Returns an error when the result is staged, foreign, malformed, or
    /// belongs to another publication authority.
    pub fn verify_reconciliation_for(
        &self,
        command: &AgentCommand,
        expected_intent: &AgentStreamPublicationIntent,
    ) -> ProtocolResult<()> {
        self.verify_for(command)?;
        expected_intent.verify()?;
        match self {
            Self::PublicationNotApplied { intent, .. }
            | Self::PublicationOutcomeUnknown { intent } => {
                if intent != expected_intent {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent stream reconciliation changed its exact intent".to_owned(),
                    ));
                }
            }
            Self::Committed { commit } => {
                let AgentCommandSource::Stream(source) = &commit.receipt.source else {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent stream reconciliation retained another source profile".to_owned(),
                    ));
                };
                let AgentStreamSource::Finalize { stream, .. } = source.as_ref() else {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent stream reconciliation retained another stream source".to_owned(),
                    ));
                };
                if !matches!(
                    stream.delivery,
                    AgentStreamDelivery::ExternalResource { .. }
                ) || stream
                    .publication_reservation
                    .as_ref()
                    .is_none_or(|reservation| reservation.intent != *expected_intent)
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent stream reconciliation has no exact external reservation".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Closed result of one workspace provider/M1 commit attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentWorkspaceCommitOutcome {
    /// The exact Agent and M1 transition committed or replayed.
    Committed {
        /// Stable semantic Agent commit.
        commit: Box<AgentCommit>,
    },
    /// A fresh Unknown observation repeated exact retained evidence, so no CAS
    /// or synthetic receipt was created.
    Unchanged {
        /// Exact uncommitted workspace command whose provider observation was a no-op.
        command_id: String,
        /// Current physical revision observed without mutation.
        observed_revision: String,
        /// Exact existing Unknown occurrence current.
        current: Box<AgentOccurrenceCurrent>,
    },
}

impl AgentWorkspaceCommitOutcome {
    /// Verify correlation with the exact workspace command and retained current.
    ///
    /// # Errors
    ///
    /// Returns an error when a commit receipt or no-op current does not match
    /// the complete command owner and closed Unknown semantics.
    pub fn verify_for(&self, command: &AgentCommand) -> ProtocolResult<()> {
        command.verify()?;
        let AgentCommandAction::Workspace(workspace) = &command.action else {
            return Err(ProtocolError::Validation(
                "Agent workspace commit outcome requires a Workspace command".to_owned(),
            ));
        };
        match self {
            Self::Committed { commit } => commit.verify_for(command),
            Self::Unchanged {
                command_id,
                observed_revision,
                current,
            } => {
                validate_sha256("Agent workspace unchanged revision", observed_revision)?;
                current.verify()?;
                if command_id != &command.command_id
                    || current.occurrence.state != AgentHostOccurrenceState::Unknown
                {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent workspace unchanged outcome changed its command or current state"
                            .to_owned(),
                    ));
                }
                verify_workspace_terminal_body_capacity(&current.occurrence)?;
                verify_workspace_occurrence_owner(
                    &current.occurrence,
                    workspace.request(),
                    matches!(
                        workspace.as_ref(),
                        AgentWorkspaceCommand::SettleEffect { .. }
                    ),
                )
            }
        }
    }

    /// Borrow the committed Agent receipt when a mutation occurred.
    #[must_use]
    pub const fn commit(&self) -> Option<&AgentCommit> {
        match self {
            Self::Committed { commit } => Some(commit),
            Self::Unchanged { .. } => None,
        }
    }
}

/// Closed post-commit projection union.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "profile",
    content = "receipt",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AgentCommandOutcome {
    /// Session metadata, update identity, and the only affected keyed entry.
    Session(AgentSessionPostcondition),
    /// Session metadata and exact occurrence current.
    Occurrence(AgentOccurrencePostcondition),
    /// Exact bounded stream postcondition.
    Stream(AgentStreamPostcondition),
    /// Exact bounded input postcondition and typed M1 receipt references.
    Input(AgentInputCheckpoint),
    /// Exact bounded workspace postcondition.
    Workspace(Box<WorkspaceScopeCheckpoint>),
}

impl AgentCommandReceipt {
    /// Construct and verify one exact bounded receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the command, source, or outcome is invalid or does
    /// not replay to one exact bounded transition.
    pub fn new(
        command: &AgentCommand,
        source: AgentCommandSource,
        outcome: AgentCommandOutcome,
    ) -> ProtocolResult<Self> {
        let receipt_id = agent_command_receipt_id(&command.command_id, &source, &outcome)?;
        let receipt = Self {
            receipt_version: AGENT_COMMAND_RECEIPT_VERSION.to_owned(),
            receipt_id,
            command_id: command.command_id.clone(),
            source,
            outcome,
        };
        receipt.verify_for(command)?;
        Ok(receipt)
    }

    /// Verify bounded before/after state by replaying the unique profile reducer.
    ///
    /// Input and workspace M1 receipt identities remain typed resolved
    /// references. The Durable Agent facade resolves and exact-matches them on
    /// commit and every authoritative typed read.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, identities, source, reducer replay,
    /// postcondition, or bounded receipt representation is invalid.
    pub fn verify_for(&self, command: &AgentCommand) -> ProtocolResult<()> {
        require_agent_version(
            "Agent command receipt",
            &self.receipt_version,
            AGENT_COMMAND_RECEIPT_VERSION,
        )?;
        command.verify()?;
        if self.command_id != command.command_id {
            return Err(ProtocolError::IdentityMismatch(
                "Agent receipt command identity does not match".to_owned(),
            ));
        }
        let expected_receipt_id =
            agent_command_receipt_id(&self.command_id, &self.source, &self.outcome)?;
        if self.receipt_id != expected_receipt_id {
            return Err(ProtocolError::IdentityMismatch(format!(
                "Agent command receipt {} does not match {expected_receipt_id}",
                self.receipt_id
            )));
        }
        let exact = match (&command.action, &self.source, &self.outcome) {
            (
                AgentCommandAction::SessionUpdate { session_id, update },
                AgentCommandSource::Session {
                    session,
                    update: source,
                },
                AgentCommandOutcome::Session(postcondition),
            ) => {
                if &session.session_id != session_id {
                    return Err(ProtocolError::IdentityMismatch(
                        "Agent Session source changed its owner".to_owned(),
                    ));
                }
                let expected = session.reduce_update(&command.command_id, update, source)?;
                postcondition.verify_for(update)?;
                &expected == postcondition
            }
            (
                AgentCommandAction::Occurrence { occurrence },
                AgentCommandSource::Occurrence(source),
                AgentCommandOutcome::Occurrence(postcondition),
            ) => {
                let expected = source.reduce(&command.command_id, occurrence)?;
                postcondition.verify_for(occurrence)?;
                &expected == postcondition
            }
            (
                AgentCommandAction::Stream(_),
                AgentCommandSource::Stream(source),
                AgentCommandOutcome::Stream(postcondition),
            ) => {
                source.verify_postcondition(command, postcondition)?;
                true
            }
            (
                AgentCommandAction::Input(input),
                AgentCommandSource::Input(source),
                AgentCommandOutcome::Input(checkpoint),
            ) => {
                let expected =
                    source.reduce(&command.command_id, input, checkpoint.wait.clone())?;
                checkpoint.verify_for(input)?;
                &expected == checkpoint
            }
            (
                AgentCommandAction::Workspace(workspace),
                AgentCommandSource::Workspace(source),
                AgentCommandOutcome::Workspace(checkpoint),
            ) => {
                source.verify_postcondition(&command.command_id, workspace, checkpoint)?;
                true
            }
            _ => false,
        };
        if !exact {
            return Err(ProtocolError::IdentityMismatch(
                "Agent receipt shape does not match its command".to_owned(),
            ));
        }
        validate_canonical_size("Agent command receipt", self, MAX_AGENT_RECEIPT_BYTES)
    }

    /// Return the exact Resource pin receipt created by external stream finalization.
    ///
    /// # Errors
    ///
    /// Returns an error when this receipt is invalid for the command or an
    /// external finalization has lost its exact Resource pin receipt.
    pub fn resource_pin_receipt_for(
        &self,
        command: &AgentCommand,
    ) -> ProtocolResult<Option<&ResourcePinReceipt>> {
        self.verify_for(command)?;
        let (
            AgentCommandAction::Stream(AgentStreamCommand::Finalize { .. }),
            AgentCommandOutcome::Stream(AgentStreamPostcondition {
                stream:
                    AgentStreamCurrent {
                        delivery: AgentStreamDelivery::ExternalResource { .. },
                        ..
                    },
                effect:
                    AgentStreamEffect::Finalized {
                        resource_pin_receipt,
                        ..
                    },
                ..
            }),
        ) = (&command.action, &self.outcome)
        else {
            return Ok(None);
        };
        resource_pin_receipt.as_ref().map(Some).ok_or_else(|| {
            ProtocolError::IdentityMismatch(
                "external Agent stream finalization lost its Resource pin receipt".to_owned(),
            )
        })
    }

    /// Return the exact Resource release created by aborting one durably
    /// `NotApplied` external publication reservation.
    ///
    /// # Errors
    ///
    /// Returns an error when this receipt is invalid for the command or the
    /// retained release is malformed.
    pub fn resource_release_receipt_for(
        &self,
        command: &AgentCommand,
    ) -> ProtocolResult<Option<&ResourceReleaseReceipt>> {
        self.verify_for(command)?;
        let (
            AgentCommandAction::Stream(AgentStreamCommand::Abort { .. }),
            AgentCommandOutcome::Stream(AgentStreamPostcondition {
                effect:
                    AgentStreamEffect::Aborted {
                        resource_release_receipt,
                        ..
                    },
                ..
            }),
        ) = (&command.action, &self.outcome)
        else {
            return Ok(None);
        };
        if let Some(receipt) = resource_release_receipt {
            receipt.verify().map_err(resource_protocol_error)?;
        }
        Ok(resource_release_receipt.as_ref())
    }
}

fn agent_command_receipt_id(
    command_id: &str,
    source: &AgentCommandSource,
    outcome: &AgentCommandOutcome,
) -> ProtocolResult<String> {
    content_id(
        AGENT_COMMAND_RECEIPT_ID_DOMAIN,
        &(AGENT_COMMAND_RECEIPT_VERSION, command_id, source, outcome),
    )
    .map_err(Into::into)
}

/// Derive the immutable Session update identity for one stream finalization.
///
/// # Errors
///
/// Returns an error when either identity is invalid or derivation fails.
pub fn agent_stream_final_update_id(session_id: &str, stream_id: &str) -> ProtocolResult<String> {
    validate_identity("Agent Session", session_id)?;
    validate_identity("Agent stream", stream_id)?;
    content_id(
        AGENT_STREAM_FINAL_UPDATE_ID_DOMAIN,
        &(session_id, stream_id),
    )
    .map_err(Into::into)
}

/// Derive the mandatory coupled-finalization receipt identity.
///
/// # Errors
///
/// Returns an error when either identity is invalid or derivation fails.
pub fn agent_stream_finalization_coupling_id(
    session_id: &str,
    stream_id: &str,
) -> ProtocolResult<String> {
    validate_identity("Agent Session", session_id)?;
    validate_identity("Agent stream", stream_id)?;
    content_id(
        AGENT_STREAM_FINALIZATION_COUPLING_ID_DOMAIN,
        &(session_id, stream_id),
    )
    .map_err(Into::into)
}

/// Derive the immutable publication catalog key for one finalized stream.
///
/// # Errors
///
/// Returns an error when either identity is invalid or derivation fails.
pub fn agent_stream_publication_key(session_id: &str, stream_id: &str) -> ProtocolResult<String> {
    validate_identity("Agent Session", session_id)?;
    validate_identity("Agent stream", stream_id)?;
    content_id(AGENT_STREAM_PUBLICATION_NAMESPACE, &(session_id, stream_id)).map_err(Into::into)
}

/// Closed core-command phase used by one workspace decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentWorkspaceCommandPhase {
    /// Propose the Plan-declared Effect.
    ProposeEffect,
    /// Pin the exact Effect implementation.
    PrepareEffect,
    /// Commit the owning scope.
    CommitScope,
    /// Authorize release after scope commit.
    AuthorizeEffect,
    /// Start the provider dispatch boundary.
    StartEffectDispatch,
    /// Settle the original Effect as applied.
    SettleEffectApplied,
    /// Settle the original Effect as not applied.
    SettleEffectNotApplied,
    /// Preserve the original Effect as unknown.
    SettleEffectUnknown,
    /// Start the provider abort dispatch after retaining its occurrence.
    StartAbortDispatch,
    /// Close the scope after provider evidence proves the abort applied.
    SettleAbortApplied,
    /// Preserve the open scope after provider evidence proves abort did not apply.
    SettleAbortNotApplied,
    /// Preserve the original abort as unknown.
    SettleAbortUnknown,
}

#[derive(Serialize)]
struct WorkspaceCommandIdentity<'a> {
    command_version: &'static str,
    run_id: &'a str,
    session_id: &'a str,
    occurrence_id: &'a str,
    phase: AgentWorkspaceCommandPhase,
}

/// Derive one workspace-internal core command identity.
///
/// # Errors
///
/// Returns an error when the workspace owner is invalid or identity derivation fails.
pub fn agent_workspace_command_id(
    request: &WorkspaceScopeRequest,
    phase: AgentWorkspaceCommandPhase,
) -> ProtocolResult<String> {
    request.verify()?;
    content_id(
        cymule_core::COMMAND_VERSION,
        &WorkspaceCommandIdentity {
            command_version: cymule_core::COMMAND_VERSION,
            run_id: &request.run_id,
            session_id: &request.session_id,
            occurrence_id: &request.occurrence_id,
            phase,
        },
    )
    .map_err(Into::into)
}

/// Derive the sole outbox claim owner for one workspace occurrence.
///
/// # Errors
///
/// Returns an error when the workspace owner is invalid or identity derivation fails.
pub fn agent_workspace_claim_owner(request: &WorkspaceScopeRequest) -> ProtocolResult<String> {
    request.verify()?;
    agent_workspace_claim_owner_unchecked(request)
}

fn agent_workspace_claim_owner_unchecked(
    request: &WorkspaceScopeRequest,
) -> ProtocolResult<String> {
    content_id(
        AGENT_WORKSPACE_CLAIM_OWNER_ID_DOMAIN,
        &(
            request.run_id.as_str(),
            request.session_id.as_str(),
            request.occurrence_id.as_str(),
        ),
    )
    .map_err(Into::into)
}

fn require_agent_version(kind: &str, actual: &str, expected: &str) -> ProtocolResult<()> {
    if actual != expected {
        return Err(ProtocolError::Validation(format!(
            "unsupported {kind} version {actual:?}; expected {expected:?}"
        )));
    }
    Ok(())
}

fn resource_protocol_error(error: crate::resource::ResourceError) -> ProtocolError {
    match error {
        crate::resource::ResourceError::Validation(message) => ProtocolError::Validation(message),
        crate::resource::ResourceError::Schema(issue) => ProtocolError::Validation(format!(
            "schema_failed: contract={} instance={} schema={}",
            issue.contract_id, issue.instance_path, issue.schema_path
        )),
        crate::resource::ResourceError::Conflict { code, message } => {
            ProtocolError::Conflict { code, message }
        }
        crate::resource::ResourceError::NotFound(message) => ProtocolError::NotFound { message },
        crate::resource::ResourceError::Substrate { code, message } => {
            ProtocolError::Substrate { code, message }
        }
        crate::resource::ResourceError::Persistence { code, message } => {
            ProtocolError::Persistence { code, message }
        }
        crate::resource::ResourceError::CommitOutcomeUnknown { message } => {
            ProtocolError::CommitOutcomeUnknown { message }
        }
        crate::resource::ResourceError::Integrity { code, message } => {
            ProtocolError::Integrity { code, message }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::resource::{
        MAX_RESOURCE_ANNOTATIONS, RESOURCE_LOCATOR_VERSION, RESOURCE_VERSION, ResourceCandidate,
        ResourceIntegrity, ResourceLifecycleReceiptRef, ResourceLocation, ResourceLocatorSet,
        ResourceShape,
    };

    fn revision(digit: char) -> String {
        format!("sha256:{}", digit.to_string().repeat(64))
    }

    fn absent_message_claim(message_id: &str) -> Vec<AgentTargetClaimSource> {
        vec![AgentTargetClaimSource {
            target: AgentTargetClaimTarget::Message {
                message_id: message_id.to_owned(),
            },
            current: None,
        }]
    }

    fn absent_tool_claim(tool_call_id: &str) -> Vec<AgentTargetClaimSource> {
        vec![AgentTargetClaimSource {
            target: AgentTargetClaimTarget::Tool {
                tool_call_id: tool_call_id.to_owned(),
            },
            current: None,
        }]
    }

    fn absent_close_claims(
        session_id: &str,
        tools: &[AgentToolCurrent],
    ) -> Vec<AgentTargetClaimSource> {
        let mut claims = tools
            .iter()
            .map(|tool| {
                let target = AgentTargetClaimTarget::Tool {
                    tool_call_id: tool.tool.tool_call_id.clone(),
                };
                (
                    agent_target_claim_key(session_id, &target).expect("claim key derives"),
                    AgentTargetClaimSource {
                        target,
                        current: None,
                    },
                )
            })
            .collect::<Vec<_>>();
        claims.sort_by(|left, right| left.0.cmp(&right.0));
        claims.into_iter().map(|(_, claim)| claim).collect()
    }

    #[test]
    fn tool_derived_id_accepts_maximal_unicode_and_binds_session_and_purpose() {
        let session = "界".repeat(512);
        let tool = "🧪".repeat(512);
        let permission =
            agent_tool_derived_id(&session, &tool, AgentToolDerivedPurpose::PermissionRequest)
                .expect("maximal Unicode tool and Session identities remain valid");
        let message = agent_tool_derived_id(&session, &tool, AgentToolDerivedPurpose::ToolMessage)
            .expect("maximal Unicode tool result identity remains valid");
        for derived in [&permission, &message] {
            cymule_core::validate_content_id("derived tool identity", derived).unwrap();
            validate_identity("derived tool identity", derived).unwrap();
            assert_eq!(derived.len(), 71);
        }
        assert_ne!(permission, message);
        assert_ne!(
            permission,
            agent_tool_derived_id(
                "session:another",
                &tool,
                AgentToolDerivedPurpose::PermissionRequest,
            )
            .unwrap(),
        );
        assert_eq!(
            permission,
            agent_tool_derived_id(&session, &tool, AgentToolDerivedPurpose::PermissionRequest)
                .unwrap(),
        );
        for (session, tool) in [("", "tool"), ("session", ""), ("session", "tool\n")] {
            assert!(
                agent_tool_derived_id(session, tool, AgentToolDerivedPurpose::ToolMessage).is_err()
            );
        }
        assert!(
            agent_tool_derived_id(
                "session",
                &"🧪".repeat(513),
                AgentToolDerivedPurpose::PermissionRequest,
            )
            .is_err()
        );
    }

    fn materialize_target_claim_after_64(
        session_id: &str,
        target: AgentTargetClaimTarget,
        mut reusable: AgentTargetClaimCurrent,
    ) -> AgentTargetClaimCurrent {
        for ordinal in 0..33_u8 {
            let reservation_id = revision(char::from(b'f' - (ordinal % 6)));
            let reservation = AgentTargetClaimTransition::new(
                session_id,
                target.clone(),
                Some(&reusable),
                AgentTargetClaimPhase::Reserved {
                    stream_id: format!("stream:target-claim:{ordinal}"),
                    reservation_id: reservation_id.clone(),
                },
                &revision(char::from(b'0' + (ordinal % 10))),
            )
            .expect("released target admits another reservation without a business cap");
            reusable = AgentTargetClaimTransition::new(
                session_id,
                target.clone(),
                Some(&reservation.current),
                AgentTargetClaimPhase::Released {
                    stream_id: format!("stream:target-claim:{ordinal}"),
                    reservation_id,
                },
                &revision(char::from(b'a' + (ordinal % 6))),
            )
            .expect("reserved target remains releasable after generation 64")
            .current;
        }
        assert!(reusable.generation > 64);
        AgentTargetClaimTransition::new(
            session_id,
            target,
            Some(&reusable),
            AgentTargetClaimPhase::Materialized,
            &revision('5'),
        )
        .expect("a target beyond generation 64 can still materialize")
        .current
    }

    #[test]
    fn target_claim_key_is_role_free_and_phase_generations_are_closed() {
        let session_id = "session:target-claim";
        let message_id = "message:target-claim";
        let user = AgentTargetClaimTarget::from_stream_target(&AgentStreamTarget::Message {
            message_id: message_id.to_owned(),
            role: MessageRole::User,
        });
        let system = AgentTargetClaimTarget::from_stream_target(&AgentStreamTarget::Message {
            message_id: message_id.to_owned(),
            role: MessageRole::System,
        });
        assert_eq!(user, system);
        assert_eq!(
            agent_target_claim_key(session_id, &user).unwrap(),
            agent_target_claim_key(session_id, &system).unwrap()
        );

        let finalize_command = revision('a');
        let reservation_id = revision('b');
        let reserved = AgentTargetClaimTransition::new(
            session_id,
            user.clone(),
            None,
            AgentTargetClaimPhase::Reserved {
                stream_id: "stream:target-claim".to_owned(),
                reservation_id: reservation_id.clone(),
            },
            &finalize_command,
        )
        .expect("absence admits one Reserved generation");
        assert_eq!(reserved.current.generation, 1);
        let materialized = AgentTargetClaimTransition::new(
            session_id,
            user.clone(),
            Some(&reserved.current),
            AgentTargetClaimPhase::Materialized,
            &finalize_command,
        )
        .expect("the same Finalize reservation materializes exactly once");
        assert_eq!(materialized.current.generation, 2);
        assert!(
            AgentTargetClaimTransition::new(
                session_id,
                user.clone(),
                Some(&materialized.current),
                AgentTargetClaimPhase::Materialized,
                &revision('c'),
            )
            .is_err()
        );

        let released = AgentTargetClaimTransition::new(
            session_id,
            user.clone(),
            Some(&reserved.current),
            AgentTargetClaimPhase::Released {
                stream_id: "stream:target-claim".to_owned(),
                reservation_id,
            },
            &revision('d'),
        )
        .expect("NotApplied Abort releases the exact reservation");
        let reused = AgentTargetClaimTransition::new(
            session_id,
            user.clone(),
            Some(&released.current),
            AgentTargetClaimPhase::Materialized,
            &revision('e'),
        )
        .expect("released target admits one later materialization generation");
        assert_eq!(reused.current.generation, released.current.generation + 1);

        let terminal = materialize_target_claim_after_64(session_id, user, released.current);
        let record = AgentTargetClaimGenerationRecord::from_current(&terminal)
            .expect("terminal generation record derives");
        record
            .verify_for(&terminal)
            .expect("generation record binds the exact claim");
    }

    #[test]
    fn target_claim_rejects_foreign_source_and_reserved_nonterminal_write() {
        let target = AgentTargetClaimTarget::Tool {
            tool_call_id: "tool:target-claim".to_owned(),
        };
        let reserved = AgentTargetClaimTransition::new(
            "session:target-claim",
            target.clone(),
            None,
            AgentTargetClaimPhase::Reserved {
                stream_id: "stream:target-claim".to_owned(),
                reservation_id: revision('1'),
            },
            &revision('2'),
        )
        .unwrap();
        assert!(
            require_unclaimed_target(&AgentTargetClaimSource {
                target: target.clone(),
                current: Some(reserved.current.clone()),
            })
            .is_err()
        );
        assert!(
            AgentTargetClaimTransition::new(
                "session:foreign",
                target,
                Some(&reserved.current),
                AgentTargetClaimPhase::Materialized,
                &revision('2'),
            )
            .is_err()
        );
    }

    #[test]
    fn structured_resource_failures_keep_their_protocol_category_and_details() {
        let cases = [
            (
                crate::resource::ResourceError::Validation("invalid descriptor".to_owned()),
                ProtocolError::Validation("invalid descriptor".to_owned()),
            ),
            (
                crate::resource::ResourceError::Schema(
                    crate::resource::ResourceSchemaIssue {
                        contract_id: "contract:test".to_owned(),
                        instance_path: "/value".to_owned(),
                        schema_path: "/properties/value/type".to_owned(),
                    },
                ),
                ProtocolError::Validation(
                    "schema_failed: contract=contract:test instance=/value schema=/properties/value/type"
                        .to_owned(),
                ),
            ),
            (
                crate::resource::ResourceError::Conflict {
                    code: "resource_alias_conflict".to_owned(),
                    message: "alias has different content".to_owned(),
                },
                ProtocolError::Conflict {
                    code: "resource_alias_conflict".to_owned(),
                    message: "alias has different content".to_owned(),
                },
            ),
            (
                crate::resource::ResourceError::Substrate {
                    code: "resource_provider_unavailable".to_owned(),
                    message: "provider did not answer".to_owned(),
                },
                ProtocolError::Substrate {
                    code: "resource_provider_unavailable".to_owned(),
                    message: "provider did not answer".to_owned(),
                },
            ),
            (
                crate::resource::ResourceError::NotFound(
                    "resource catalog entry is absent".to_owned(),
                ),
                ProtocolError::NotFound {
                    message: "resource catalog entry is absent".to_owned(),
                },
            ),
            (
                crate::resource::ResourceError::Persistence {
                    code: "resource_state_write_failed".to_owned(),
                    message: "state root write failed".to_owned(),
                },
                ProtocolError::Persistence {
                    code: "resource_state_write_failed".to_owned(),
                    message: "state root write failed".to_owned(),
                },
            ),
            (
                crate::resource::ResourceError::CommitOutcomeUnknown {
                    message: "receipt response was lost".to_owned(),
                },
                ProtocolError::CommitOutcomeUnknown {
                    message: "receipt response was lost".to_owned(),
                },
            ),
            (
                crate::resource::ResourceError::Integrity {
                    code: "resource_digest_mismatch".to_owned(),
                    message: "resource digest differs".to_owned(),
                },
                ProtocolError::Integrity {
                    code: "resource_digest_mismatch".to_owned(),
                    message: "resource digest differs".to_owned(),
                },
            ),
        ];

        for (source, expected) in cases {
            assert_eq!(resource_protocol_error(source), expected);
        }
    }

    fn input_owner(site_id: &str) -> WaitOwner {
        WaitOwner {
            invocation_id: "invocation:input".to_owned(),
            definition_id: "definition:input".to_owned(),
            site_id: site_id.to_owned(),
            region_path: vec![0],
            step_index: 0,
            bind: Some("local:input".to_owned()),
        }
    }

    fn context_request(session_id: &str) -> ContextRequest {
        ContextRequest {
            session_id: session_id.to_owned(),
            source_message_head: None,
            source_message_count: 0,
            budget: 1,
            scan_limits: AgentContextScanLimits {
                max_entries: 1,
                max_canonical_bytes: 1024,
            },
        }
    }

    fn message_update(update_id: &str, message_id: &str, text: String) -> AgentUpdate {
        AgentUpdate::Message {
            update_id: update_id.to_owned(),
            message: AgentMessage {
                message_id: message_id.to_owned(),
                role: MessageRole::Agent,
                content: vec![ContentBlock::Text { text }],
            },
        }
    }

    fn external_publication() -> ResourcePublication {
        external_publication_with_digest('a')
    }

    fn external_publication_with_digest(digest: char) -> ResourcePublication {
        let resource = ResourceCandidate {
            resource_version: RESOURCE_VERSION.to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: revision(digest),
                size: 1,
            },
            manifest: None,
            annotations: BTreeMap::new(),
        }
        .seal()
        .expect("external Resource seals");
        ResourcePublication {
            locators: ResourceLocatorSet {
                locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
                resource_id: resource.resource_id.clone(),
                resolver_binding: "resolver:test/1".to_owned(),
                locations: vec![ResourceLocation::Opaque {
                    reference: "object:test/1".to_owned(),
                }],
            },
            resource,
        }
    }

    fn maximal_metadata_external_publication() -> ResourcePublication {
        let mut annotations = BTreeMap::new();
        for index in 0..MAX_RESOURCE_ANNOTATIONS {
            annotations.insert(format!("metadata-{index:02}"), "m".repeat(4096));
        }
        let resource = ResourceCandidate {
            resource_version: RESOURCE_VERSION.to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: revision('a'),
                size: 1,
            },
            manifest: None,
            annotations,
        }
        .seal()
        .expect("maximum-count external Resource metadata seals");
        ResourcePublication {
            locators: ResourceLocatorSet {
                locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
                resource_id: resource.resource_id.clone(),
                resolver_binding: "resolver:test/1".to_owned(),
                locations: vec![ResourceLocation::Opaque {
                    reference: "object:maximal-metadata/1".to_owned(),
                }],
            },
            resource,
        }
    }

    #[test]
    fn external_message_publication_cannot_expand_the_admitted_resource_wrapper() {
        let forged_publication = maximal_metadata_external_publication();
        forged_publication
            .verify()
            .expect("maximum-count external publication is independently legal");
        let session = AgentSessionCurrent::new("session:external-wrapper-capacity")
            .expect("capacity Session constructs");
        let target_id = "🧪".repeat(512);
        let target = AgentStreamTarget::Message {
            message_id: target_id,
            role: MessageRole::Agent,
        };
        let target_source = AgentStreamTargetSource::Message { current: None };
        let content = AgentStreamPublicationContent {
            media_type: forged_publication.resource.media_type.clone(),
            digest: revision('a'),
            size: 1,
        };
        let admitted_resource = external_stream_resource_handle(&content)
            .expect("Open uniquely derives the semantic Resource Handle");
        let final_update = stream_finalization_update(
            &session.session_id,
            "stream:external-wrapper-capacity",
            &target,
            &target_source,
            &[ContentBlock::ResourceHandle {
                resource: Box::new(admitted_resource),
            }],
        )
        .expect("admitted Resource constructs the real final update");
        let final_bytes = cymule_core::canonical_bytes(&final_update)
            .expect("admitted final update encodes")
            .len();
        assert!(final_bytes <= MAX_AGENT_VALUE_BYTES);
        let open = AgentStreamCommand::Open {
            session_id: session.session_id.clone(),
            stream_id: "stream:external-wrapper-capacity".to_owned(),
            target,
            delivery: AgentStreamDelivery::ExternalResource {
                resolver_binding: "resolver:test/1".to_owned(),
                content,
            },
        };
        let command = AgentCommand::new(revision('2'), AgentCommandAction::Stream(open.clone()))
            .expect("external capacity Open command seals");
        let opened = AgentStreamSource::Open {
            session,
            stream: None,
            target: target_source.clone(),
        }
        .reduce(&command.command_id, &open)
        .expect("Message Open admits its unique bounded Resource wrapper");
        assert_eq!(opened.stream.final_update_bytes, final_bytes as u64);
        let AgentStreamEffect::Opened { session } = opened.effect else {
            panic!("Open returns the exact Session current")
        };
        let finalize = AgentStreamCommand::Finalize {
            session_id: open.session_id().to_owned(),
            stream_id: open.stream_id().to_owned(),
        };
        let finalize_command =
            AgentCommand::new(revision('3'), AgentCommandAction::Stream(finalize.clone()))
                .expect("Finalize command seals");
        let source = AgentStreamSource::Finalize {
            session,
            stream: opened.stream,
            chunks: Vec::new(),
            target: target_source,
            update: None,
            resource: None,
            target_claim: None,
        };
        let mut providers = TestAgentProviders {
            publication: Some(forged_publication),
            ..TestAgentProviders::default()
        };
        let (_, reservation) = reserve_external_stream_source(&finalize_command, source);
        assert!(execute_agent_stream_publication(&reservation, &mut providers).is_err());
        assert_eq!(providers.publication_calls, 1);
    }

    fn near_limit_tool_stream_target() -> (
        AgentSessionCurrent,
        AgentStreamTarget,
        AgentStreamTargetSource,
    ) {
        let mut session = AgentSessionCurrent::new("session:external-tool-capacity")
            .expect("capacity Session constructs");
        let target_id = "🧪".repeat(512);
        let update_id = "update:external-capacity:111111";
        let probe = AgentUpdate::Tool {
            update_id: update_id.to_owned(),
            tool: ToolCall {
                tool_call_id: target_id.clone(),
                operation: "workspace.read".to_owned(),
                status: ToolCallStatus::InProgress,
                input: serde_json::json!({"retained": ""}),
                output: None,
                locations: vec!["source:near-limit-input".to_owned()],
            },
        };
        let padding = MAX_AGENT_VALUE_BYTES
            - cymule_core::canonical_bytes(&probe)
                .expect("Tool capacity probe encodes")
                .len();
        let input = serde_json::json!({"retained": "i".repeat(padding)});
        let mut current = None;
        for (command_digit, status, update_id) in [
            (
                '1',
                ToolCallStatus::Pending,
                "update:external-capacity:000000",
            ),
            ('2', ToolCallStatus::InProgress, update_id),
        ] {
            let update = AgentUpdate::Tool {
                update_id: update_id.to_owned(),
                tool: ToolCall {
                    tool_call_id: target_id.clone(),
                    operation: "workspace.read".to_owned(),
                    status,
                    input: input.clone(),
                    output: None,
                    locations: vec!["source:near-limit-input".to_owned()],
                },
            };
            let postcondition = session
                .reduce_update(
                    &revision(command_digit),
                    &update,
                    &AgentSessionUpdateSource {
                        update: None,
                        entry: AgentSessionEntrySource::Tool { current },
                        target_claims: absent_tool_claim(&target_id),
                    },
                )
                .expect("near-limit Tool lifecycle advances");
            let AgentSessionUpdateEffect::Tool { current: next } = postcondition.effect else {
                panic!("Tool update returns its exact current")
            };
            current = Some(next);
            session = postcondition.session;
        }
        let target = AgentStreamTarget::Tool {
            tool_call_id: target_id,
        };
        let target_source = AgentStreamTargetSource::Tool { current };
        (session, target, target_source)
    }

    #[test]
    fn external_tool_wrapper_capacity_is_rejected_before_publication() {
        let (session, target, target_source) = near_limit_tool_stream_target();
        let content = AgentStreamPublicationContent {
            media_type: "application/octet-stream".to_owned(),
            digest: revision('a'),
            size: 1,
        };
        let final_update = stream_finalization_update(
            &session.session_id,
            "stream:external-tool-capacity",
            &target,
            &target_source,
            &[ContentBlock::ResourceHandle {
                resource: Box::new(
                    external_stream_resource_handle(&content)
                        .expect("external Resource Handle derives"),
                ),
            }],
        )
        .expect("external Tool final update constructs");
        let final_bytes = cymule_core::canonical_bytes(&final_update)
            .expect("external Tool final update encodes")
            .len();
        assert!(final_bytes > MAX_AGENT_VALUE_BYTES);
        let open = AgentStreamCommand::Open {
            session_id: session.session_id.clone(),
            stream_id: "stream:external-tool-capacity".to_owned(),
            target,
            delivery: AgentStreamDelivery::ExternalResource {
                resolver_binding: "resolver:test/1".to_owned(),
                content,
            },
        };
        let command = AgentCommand::new(revision('3'), AgentCommandAction::Stream(open.clone()))
            .expect("external Tool Open command seals");
        let opened = AgentStreamSource::Open {
            session,
            stream: None,
            target: target_source,
        }
        .reduce(&command.command_id, &open);
        assert!(
            matches!(opened, Err(ProtocolError::Validation(_))),
            "Open admitted an external Tool final update occupying {final_bytes} canonical bytes"
        );
    }

    fn open_message_stream(
        open: &AgentStreamCommand,
        source_revision: char,
    ) -> (AgentSessionCurrent, AgentStreamCurrent) {
        let command = AgentCommand::new(
            revision(source_revision),
            AgentCommandAction::Stream(open.clone()),
        )
        .expect("open command seals");
        let postcondition = AgentStreamSource::Open {
            session: AgentSessionCurrent::new(open.session_id()).expect("Session constructs"),
            stream: None,
            target: AgentStreamTargetSource::Message { current: None },
        }
        .reduce(&command.command_id, open)
        .expect("message stream opens");
        let AgentStreamEffect::Opened { session } = postcondition.effect else {
            panic!("open effect shape changed")
        };
        (session, postcondition.stream)
    }

    fn external_stream_finalize_fixture() -> (AgentCommand, AgentStreamCommand, AgentStreamSource) {
        external_stream_finalize_fixture_for(
            "session:external",
            "stream:external",
            "message:external",
        )
    }

    fn external_stream_finalize_fixture_for(
        session_id: &str,
        stream_id: &str,
        message_id: &str,
    ) -> (AgentCommand, AgentStreamCommand, AgentStreamSource) {
        let open = AgentStreamCommand::Open {
            session_id: session_id.to_owned(),
            stream_id: stream_id.to_owned(),
            target: AgentStreamTarget::Message {
                message_id: message_id.to_owned(),
                role: MessageRole::Agent,
            },
            delivery: AgentStreamDelivery::ExternalResource {
                resolver_binding: "resolver:test/1".to_owned(),
                content: AgentStreamPublicationContent {
                    media_type: "application/octet-stream".to_owned(),
                    digest: revision('a'),
                    size: 1,
                },
            },
        };
        let (session, stream) = open_message_stream(&open, '1');
        let finalize = AgentStreamCommand::Finalize {
            session_id: session_id.to_owned(),
            stream_id: stream_id.to_owned(),
        };
        let command =
            AgentCommand::new(revision('3'), AgentCommandAction::Stream(finalize.clone()))
                .expect("finalize command seals");
        let source = AgentStreamSource::Finalize {
            session,
            stream,
            chunks: Vec::new(),
            target: AgentStreamTargetSource::Message { current: None },
            update: None,
            resource: None,
            target_claim: None,
        };
        (command, finalize, source)
    }

    fn reserve_external_stream_source(
        command: &AgentCommand,
        mut source: AgentStreamSource,
    ) -> (AgentStreamSource, AgentStreamPublicationReservation) {
        let (_, profile_pin) = prepare_agent_stream_publication(&source, command)
            .expect("external publication selectors derive before I/O");
        let AgentStreamSource::Finalize { resource, .. } = &mut source else {
            panic!("fixture is an external finalization")
        };
        *resource = Some(Box::new(AgentStreamResourceSource {
            retention: None,
            pin: None,
        }));
        let reserved = reserve_agent_stream_publication(&source, command)
            .expect("external publication reserves its physical family");
        let reservation = reserved
            .stream
            .publication_reservation
            .as_ref()
            .expect("reservation is embedded in the stream current")
            .as_ref()
            .clone();
        assert_eq!(profile_pin.pin, reservation.resource_pin_receipt.pin);
        let origin = ResourceLifecycleReceiptRef::from_agent_publication_reservation(
            command.command_id.clone(),
            reservation.intent.session_id().to_owned(),
            reservation.intent.stream_id().to_owned(),
            reservation.reservation_id.clone(),
        )
        .expect("reservation lifecycle edge seals");
        let resource = crate::resource::project_resource_pin_reservation_receipt(
            &reservation.resource_pin_receipt,
            origin,
            None,
            None,
        )
        .expect("reservation projects its exact Resource currents");
        let AgentStreamSource::Finalize {
            stream,
            resource: source_resource,
            target_claim,
            ..
        } = &mut source
        else {
            unreachable!("fixture shape was checked above")
        };
        *stream = reserved.stream;
        *source_resource = Some(Box::new(AgentStreamResourceSource {
            retention: Some(resource.retention),
            pin: Some(resource.pin),
        }));
        *target_claim = Some(Box::new(reserved.target_claim.current));
        (source, reservation)
    }

    struct WorkspaceTestFixture {
        request: WorkspaceScopeRequest,
        binding: AgentHostBinding,
        effect_intent_id: String,
        obligation_id: String,
        start: AgentWorkspaceCommand,
        command: AgentCommand,
        source: AgentWorkspaceSource,
    }

    fn workspace_test_fixture() -> WorkspaceTestFixture {
        let mut request = WorkspaceScopeRequest {
            session_id: "session:workspace-authority".to_owned(),
            run_id: "run:workspace-authority".to_owned(),
            scope_id: "scope:workspace-authority".to_owned(),
            occurrence_id: "occurrence:workspace-authority".to_owned(),
            change_id: "change:workspace-authority".to_owned(),
            overlay: cymule_core::artifact_ref("workspace/overlay", b"overlay")
                .expect("overlay Artifact derives"),
            operation: "workspace.commit".to_owned(),
            invocation_id: "invocation:workspace-authority".to_owned(),
            site_id: "site:workspace-authority".to_owned(),
            occurrence_key: "primary".to_owned(),
            dispatch_lease: None,
        };
        request.dispatch_lease = Some(
            AgentWorkspaceDispatchLeaseRequest::new(
                &request,
                ClockObservationRef {
                    clock_version: cymule_durable_protocol::CLOCK_OBSERVATION_VERSION.to_owned(),
                    observation_id: revision('9'),
                    source_id: "clock:workspace-authority".to_owned(),
                    source_generation: revision('8'),
                    scope: execution_clock_scope(&request.run_id)
                        .expect("workspace Clock scope derives"),
                },
                60,
            )
            .expect("workspace dispatch lease derives"),
        );
        let execution_binding = cymule_core::artifact_ref(
            EXECUTION_BINDING_ARTIFACT_KIND,
            b"workspace execution binding",
        )
        .expect("execution binding Artifact derives");
        let operation_occurrence_binding = "operation-binding:workspace-authority/1";
        let binding = AgentHostBinding::m1_effect_operation(
            "workspace-host:test/1",
            execution_binding.clone(),
            request.operation.clone(),
            operation_occurrence_binding,
        )
        .expect("binding closes over exact M1 operation");
        let effect_intent_id = "effect:workspace-authority".to_owned();
        let obligation_id = cymule_core::effect_obligation_id(&effect_intent_id)
            .expect("obligation identity derives");
        let start = AgentWorkspaceCommand::StartEffect {
            request: request.clone(),
            effect_intent_id: effect_intent_id.clone(),
            execution_binding,
            operation_occurrence_binding: operation_occurrence_binding.to_owned(),
        };
        let command = AgentCommand::new(
            revision('1'),
            AgentCommandAction::Workspace(Box::new(start.clone())),
        )
        .expect("workspace start command seals");
        let source = AgentWorkspaceSource {
            occurrence: AgentOccurrenceSource {
                session: AgentSessionCurrent::new(&request.session_id)
                    .expect("Session current constructs"),
                current: None,
            },
        };
        WorkspaceTestFixture {
            request,
            binding,
            effect_intent_id,
            obligation_id,
            start,
            command,
            source,
        }
    }

    fn start_workspace(fixture: &WorkspaceTestFixture) -> WorkspaceScopeCheckpoint {
        let mut providers = TestAgentProviders {
            workspace_binding: Some(fixture.binding.clone()),
            ..TestAgentProviders::default()
        };
        let product =
            execute_agent_workspace_provider(&fixture.source, &fixture.command, &mut providers)
                .expect("registered workspace binding resolves");
        assert_eq!(providers.binding_calls, 1);
        fixture
            .source
            .reduce_with_provider(
                &fixture.command.command_id,
                &fixture.start,
                &product,
                AgentWorkspaceM1Witness {
                    run_id: fixture.request.run_id.clone(),
                    scope_id: fixture.request.scope_id.clone(),
                    phase: AgentWorkspaceCommandPhase::StartEffectDispatch,
                    continuation_digest: revision('2'),
                    effect_intent_id: Some(fixture.effect_intent_id.clone()),
                    obligation_id: Some(fixture.obligation_id.clone()),
                    m1_receipt_id: revision('3'),
                },
            )
            .expect("workspace start couples provider binding and M1 receipt")
    }

    #[derive(Default)]
    struct TestAgentProviders {
        publication: Option<ResourcePublication>,
        publication_observation: Option<AgentStreamPublicationObservation>,
        reconciliation_observation: Option<AgentStreamPublicationObservation>,
        workspace_binding: Option<AgentHostBinding>,
        workspace_resolution: Option<AgentOccurrenceResolution>,
        workspace_artifacts: Vec<cymule_core::ArtifactRecord>,
        publication_calls: usize,
        publication_intents: Vec<AgentStreamPublicationIntent>,
        publication_dispatches: BTreeMap<String, AgentStreamPublicationObservation>,
        publication_observation_calls: usize,
        binding_calls: usize,
        workspace_dispatch_calls: usize,
        observation_calls: usize,
    }

    impl AgentProviders for TestAgentProviders {
        fn publish_agent_stream(
            &mut self,
            dispatch: &AgentStreamPublicationReservation,
        ) -> ProtocolResult<AgentStreamPublicationObservation> {
            self.publication_calls += 1;
            self.publication_intents.push(dispatch.intent.clone());
            if let Some(observation) = self.publication_dispatches.get(&dispatch.dispatch_id) {
                return Ok(observation.clone());
            }
            if let Some(observation) = self.publication_observation.clone() {
                if !matches!(observation, AgentStreamPublicationObservation::Unknown) {
                    self.publication_dispatches
                        .insert(dispatch.dispatch_id.clone(), observation.clone());
                }
                return Ok(observation);
            }
            let observation = self
                .publication
                .clone()
                .map(|publication| AgentStreamPublicationObservation::Published {
                    publication: Box::new(publication),
                })
                .ok_or_else(|| {
                    ProtocolError::Validation("test stream provider has no publication".to_owned())
                })?;
            self.publication_dispatches
                .insert(dispatch.dispatch_id.clone(), observation.clone());
            Ok(observation)
        }

        fn reconcile_agent_stream_publication(
            &mut self,
            dispatch: &AgentStreamPublicationReservation,
        ) -> ProtocolResult<AgentStreamPublicationObservation> {
            self.publication_observation_calls += 1;
            self.publication_intents.push(dispatch.intent.clone());
            if let Some(observation) = self.publication_dispatches.get(&dispatch.dispatch_id) {
                return Ok(observation.clone());
            }
            if let Some(observation) = self.reconciliation_observation.clone() {
                if !matches!(observation, AgentStreamPublicationObservation::Unknown) {
                    self.publication_dispatches
                        .insert(dispatch.dispatch_id.clone(), observation.clone());
                }
                return Ok(observation);
            }
            let observation = self
                .publication
                .clone()
                .map(|publication| AgentStreamPublicationObservation::Published {
                    publication: Box::new(publication),
                })
                .ok_or_else(|| {
                    ProtocolError::Validation("test stream provider has no publication".to_owned())
                })?;
            self.publication_dispatches
                .insert(dispatch.dispatch_id.clone(), observation.clone());
            Ok(observation)
        }

        fn bind_agent_workspace(
            &mut self,
            _command: &AgentWorkspaceCommand,
        ) -> ProtocolResult<AgentHostBinding> {
            self.binding_calls += 1;
            self.workspace_binding.clone().ok_or_else(|| {
                ProtocolError::Validation("test workspace provider has no binding".to_owned())
            })
        }

        fn observe_agent_workspace(
            &mut self,
            _command: &AgentWorkspaceCommand,
            _occurrence: &AgentHostOccurrence,
        ) -> ProtocolResult<AgentWorkspaceObservation> {
            self.observation_calls += 1;
            let resolution = self.workspace_resolution.clone().ok_or_else(|| {
                ProtocolError::Validation("test workspace provider has no observation".to_owned())
            })?;
            Ok(AgentWorkspaceObservation {
                resolution,
                artifacts: self.workspace_artifacts.clone(),
            })
        }

        fn dispatch_agent_workspace(
            &mut self,
            _command: &AgentWorkspaceCommand,
            occurrence: &AgentHostOccurrence,
        ) -> ProtocolResult<AgentWorkspaceSubmission> {
            assert_eq!(occurrence.state, AgentHostOccurrenceState::Started);
            self.workspace_dispatch_calls += 1;
            Ok(AgentWorkspaceSubmission::Submitted)
        }
    }

    #[test]
    fn direct_session_update_cannot_mutate_elicitation() {
        let request = ElicitationRequest {
            request_id: "elicitation:one".to_owned(),
            schema: Value::Bool(true),
            prompt: Vec::new(),
        };
        let direct = AgentCommandAction::SessionUpdate {
            session_id: "session:input".to_owned(),
            update: AgentUpdate::Elicitation {
                update_id: "update:input:bypass".to_owned(),
                elicitation: ElicitationProjection {
                    wait_id: "wait:input".to_owned(),
                    request: request.clone(),
                    response: None,
                },
            },
        };
        let AgentCommandAction::SessionUpdate {
            update: direct_update,
            ..
        } = &direct
        else {
            unreachable!("fixture is a direct Session update")
        };
        assert!(
            AgentSessionCurrent::new("session:input")
                .expect("Session current constructs")
                .reduce_update(
                    &revision('1'),
                    direct_update,
                    &AgentSessionUpdateSource {
                        update: None,
                        entry: AgentSessionEntrySource::Metadata,
                        target_claims: Vec::new(),
                    },
                )
                .is_err()
        );
        assert!(AgentCommand::new(revision('1'), direct).is_err());
    }

    #[test]
    fn input_closes_first_suspension_and_last_completion_atomically() {
        let request = ElicitationRequest {
            request_id: "elicitation:one".to_owned(),
            schema: Value::Bool(true),
            prompt: Vec::new(),
        };
        let owner = input_owner("site:input");
        let suspend = AgentInputCommand::Suspend {
            session_id: "session:input".to_owned(),
            wait_id: "wait:input".to_owned(),
            expected_run_id: "run:input".to_owned(),
            expected_owner: owner.clone(),
            request: request.clone(),
        };
        let suspend_command =
            AgentCommand::new(revision('1'), AgentCommandAction::Input(suspend.clone()))
                .expect("suspend command seals");
        let suspend_source = AgentInputSource::Suspend {
            session: AgentSessionCurrent::new("session:input").expect("Session current constructs"),
            elicitation: None,
        };
        let suspended = suspend_source
            .reduce(
                &suspend_command.command_id,
                &suspend,
                AgentInputWaitWitness::Suspended {
                    run_id: "run:input".to_owned(),
                    owner: owner.clone(),
                    suspension_receipt_id: revision('2'),
                },
            )
            .expect("first input suspension reduces atomically");
        assert_eq!(suspended.session.state, AgentState::RequiresAction);
        assert_eq!(suspended.session.pending_elicitation_count, 1);
        AgentCommandReceipt::new(
            &suspend_command,
            AgentCommandSource::Input(suspend_source),
            AgentCommandOutcome::Input(suspended.clone()),
        )
        .expect("suspension receipt replays exact before and after");

        let response = ElicitationResponse {
            request_id: request.request_id,
            accepted: false,
            value: None,
            occurrence_binding: "binding:input/1".to_owned(),
        };
        let complete = AgentInputCommand::Complete {
            session_id: "session:input".to_owned(),
            wait_id: "wait:input".to_owned(),
            expected_run_id: "run:input".to_owned(),
            expected_owner: owner.clone(),
            response: response.clone(),
        };
        let complete_command =
            AgentCommand::new(revision('3'), AgentCommandAction::Input(complete.clone()))
                .expect("complete command seals");
        let complete_source = AgentInputSource::Complete {
            session: suspended.session,
            elicitation: suspended.elicitation,
        };
        let result = cymule_core::artifact_ref(
            WAIT_RESULT_ARTIFACT_KIND,
            &AgentInputResult::from_response(&response)
                .expect("result derives")
                .canonical_bytes()
                .expect("result encodes"),
        )
        .expect("result Artifact derives");
        let completed = complete_source
            .reduce(
                &complete_command.command_id,
                &complete,
                AgentInputWaitWitness::Completed {
                    run_id: "run:input".to_owned(),
                    owner,
                    suspension_receipt_id: revision('2'),
                    completion_receipt_id: revision('4'),
                    result,
                },
            )
            .expect("last input completion reduces atomically");
        assert_eq!(completed.session.state, AgentState::Running);
        assert_eq!(completed.session.pending_elicitation_count, 0);
        AgentCommandReceipt::new(
            &complete_command,
            AgentCommandSource::Input(complete_source),
            AgentCommandOutcome::Input(completed),
        )
        .expect("completion receipt replays exact before and after");
    }

    #[test]
    fn input_rejects_closed_session_and_wrong_owner() {
        let request = ElicitationRequest {
            request_id: "elicitation:closed".to_owned(),
            schema: serde_json::json!({"type": "string"}),
            prompt: Vec::new(),
        };
        let owner = input_owner("site:closed");
        let suspend = AgentInputCommand::Suspend {
            session_id: "session:closed".to_owned(),
            wait_id: "wait:closed".to_owned(),
            expected_run_id: "run:closed".to_owned(),
            expected_owner: owner.clone(),
            request,
        };
        let command = AgentCommand::new(revision('1'), AgentCommandAction::Input(suspend.clone()))
            .expect("command seals");
        let mut closed = AgentSessionCurrent::new("session:closed").expect("Session constructs");
        closed.state = AgentState::Closed;
        closed.verify().expect("empty closed Session is valid");
        let source = AgentInputSource::Suspend {
            session: closed,
            elicitation: None,
        };
        assert!(
            source
                .reduce(
                    &command.command_id,
                    &suspend,
                    AgentInputWaitWitness::Suspended {
                        run_id: "run:closed".to_owned(),
                        owner,
                        suspension_receipt_id: revision('2'),
                    },
                )
                .is_err()
        );

        let source = AgentInputSource::Suspend {
            session: AgentSessionCurrent::new("session:closed").expect("Session constructs"),
            elicitation: None,
        };
        assert!(
            source
                .reduce(
                    &command.command_id,
                    &suspend,
                    AgentInputWaitWitness::Suspended {
                        run_id: "run:different".to_owned(),
                        owner: input_owner("site:closed"),
                        suspension_receipt_id: revision('2'),
                    },
                )
                .is_err()
        );
        let mut wrong_owner = input_owner("site:closed");
        wrong_owner.site_id = "site:different".to_owned();
        assert!(
            source
                .reduce(
                    &command.command_id,
                    &suspend,
                    AgentInputWaitWitness::Suspended {
                        run_id: "run:closed".to_owned(),
                        owner: wrong_owner,
                        suspension_receipt_id: revision('2'),
                    },
                )
                .is_err()
        );
    }

    fn pending_string_input() -> AgentInputCheckpoint {
        let suspend = AgentInputCommand::Suspend {
            session_id: "session:closed".to_owned(),
            wait_id: "wait:closed".to_owned(),
            expected_run_id: "run:closed".to_owned(),
            expected_owner: input_owner("site:closed"),
            request: ElicitationRequest {
                request_id: "elicitation:closed".to_owned(),
                schema: serde_json::json!({"type": "string"}),
                prompt: Vec::new(),
            },
        };
        let command = AgentCommand::new(revision('1'), AgentCommandAction::Input(suspend.clone()))
            .expect("command seals");
        AgentInputSource::Suspend {
            session: AgentSessionCurrent::new("session:closed").expect("Session constructs"),
            elicitation: None,
        }
        .reduce(
            &command.command_id,
            &suspend,
            AgentInputWaitWitness::Suspended {
                run_id: "run:closed".to_owned(),
                owner: input_owner("site:closed"),
                suspension_receipt_id: revision('2'),
            },
        )
        .expect("valid suspension reduces")
    }

    #[test]
    fn input_completion_rejects_wrong_wait_result() {
        let suspended = pending_string_input();
        let declined = ElicitationResponse {
            request_id: "elicitation:closed".to_owned(),
            accepted: false,
            value: None,
            occurrence_binding: "binding:closed/1".to_owned(),
        };
        let complete = AgentInputCommand::Complete {
            session_id: "session:closed".to_owned(),
            wait_id: "wait:closed".to_owned(),
            expected_run_id: "run:closed".to_owned(),
            expected_owner: input_owner("site:closed"),
            response: declined,
        };
        let complete_command =
            AgentCommand::new(revision('3'), AgentCommandAction::Input(complete.clone()))
                .expect("completion command seals");
        let complete_source = AgentInputSource::Complete {
            session: suspended.session.clone(),
            elicitation: suspended.elicitation.clone(),
        };
        let wrong_result = cymule_core::artifact_ref(WAIT_RESULT_ARTIFACT_KIND, b"wrong")
            .expect("wrong result Artifact derives");
        assert!(
            complete_source
                .reduce(
                    &complete_command.command_id,
                    &complete,
                    AgentInputWaitWitness::Completed {
                        run_id: "run:closed".to_owned(),
                        owner: input_owner("site:closed"),
                        suspension_receipt_id: revision('2'),
                        completion_receipt_id: revision('4'),
                        result: wrong_result,
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn input_completion_rejects_value_outside_request_schema() {
        let suspended = pending_string_input();
        let complete_source = AgentInputSource::Complete {
            session: suspended.session,
            elicitation: suspended.elicitation,
        };
        let invalid_response = ElicitationResponse {
            request_id: "elicitation:closed".to_owned(),
            accepted: true,
            value: Some(Value::from(1)),
            occurrence_binding: "binding:closed/1".to_owned(),
        };
        let invalid_complete = AgentInputCommand::Complete {
            session_id: "session:closed".to_owned(),
            wait_id: "wait:closed".to_owned(),
            expected_run_id: "run:closed".to_owned(),
            expected_owner: input_owner("site:closed"),
            response: invalid_response.clone(),
        };
        let invalid_command = AgentCommand::new(
            revision('3'),
            AgentCommandAction::Input(invalid_complete.clone()),
        )
        .expect("response-local completion command seals");
        let invalid_result = cymule_core::artifact_ref(
            WAIT_RESULT_ARTIFACT_KIND,
            &AgentInputResult::from_response(&invalid_response)
                .expect("response-local result derives")
                .canonical_bytes()
                .expect("result encodes"),
        )
        .expect("invalid-schema result Artifact derives");
        assert!(
            complete_source
                .reduce(
                    &invalid_command.command_id,
                    &invalid_complete,
                    AgentInputWaitWitness::Completed {
                        run_id: "run:closed".to_owned(),
                        owner: input_owner("site:closed"),
                        suspension_receipt_id: revision('2'),
                        completion_receipt_id: revision('4'),
                        result: invalid_result,
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn receipt_rejects_unrelated_postcondition_mutation_and_duplicate_update_identity() {
        let update = AgentUpdate::State {
            update_id: "update:state:one".to_owned(),
            state: AgentState::Running,
            stop_reason: None,
        };
        let command = AgentCommand::new(
            revision('1'),
            AgentCommandAction::SessionUpdate {
                session_id: "session:receipt".to_owned(),
                update: update.clone(),
            },
        )
        .expect("Session command seals");
        let predecessor_command_id = content_id(
            "cymule.agent-command-id/1",
            &(command.source_revision.as_str(), &command.action),
        )
        .expect("predecessor command identity derives");
        assert_ne!(command.command_id, predecessor_command_id);
        let mut predecessor_command = command.clone();
        predecessor_command.command_version = "cymule.agent-command/3".to_owned();
        assert!(predecessor_command.verify().is_err());
        let session = AgentSessionCurrent::new("session:receipt").expect("Session constructs");
        let source = AgentSessionUpdateSource {
            update: None,
            entry: AgentSessionEntrySource::Metadata,
            target_claims: Vec::new(),
        };
        let postcondition = session
            .reduce_update(&command.command_id, &update, &source)
            .expect("update reduces");
        let receipt = AgentCommandReceipt::new(
            &command,
            AgentCommandSource::Session {
                session: session.clone(),
                update: source,
            },
            AgentCommandOutcome::Session(postcondition.clone()),
        )
        .expect("exact receipt verifies");
        let predecessor_receipt_id = content_id(
            "cymule.agent-command-receipt-id/1",
            &(
                predecessor_command_id.as_str(),
                &receipt.source,
                &receipt.outcome,
            ),
        )
        .expect("predecessor receipt identity derives");
        assert_ne!(receipt.receipt_id, predecessor_receipt_id);
        let mut predecessor = receipt;
        predecessor.receipt_version = "cymule.agent-command-receipt/4".to_owned();
        assert!(predecessor.verify_for(&command).is_err());

        let mut unrelated = postcondition.clone();
        unrelated.session.plan = Some(AgentPlan {
            plan_id: "plan:unrelated".to_owned(),
            entries: Vec::new(),
        });
        assert!(
            AgentCommandReceipt::new(
                &command,
                AgentCommandSource::Session {
                    session: session.clone(),
                    update: AgentSessionUpdateSource {
                        update: None,
                        entry: AgentSessionEntrySource::Metadata,
                        target_claims: Vec::new(),
                    },
                },
                AgentCommandOutcome::Session(unrelated),
            )
            .is_err()
        );

        let duplicate_source = AgentSessionUpdateSource {
            update: Some(postcondition.update),
            entry: AgentSessionEntrySource::Metadata,
            target_claims: Vec::new(),
        };
        assert!(
            postcondition
                .session
                .reduce_update(&revision('3'), &update, &duplicate_source)
                .is_err()
        );
    }

    #[test]
    fn direct_session_updates_reject_new_command_no_ops() {
        let session = AgentSessionCurrent::new("session:no-op").expect("Session constructs");
        let metadata = AgentSessionUpdateSource {
            update: None,
            entry: AgentSessionEntrySource::Metadata,
            target_claims: Vec::new(),
        };
        let running = AgentUpdate::State {
            update_id: "update:no-op:running".to_owned(),
            state: AgentState::Running,
            stop_reason: None,
        };
        let running = session
            .reduce_update(&revision('1'), &running, &metadata)
            .expect("first state change reduces")
            .session;
        assert!(
            running
                .reduce_update(
                    &revision('2'),
                    &AgentUpdate::State {
                        update_id: "update:no-op:running-again".to_owned(),
                        state: AgentState::Running,
                        stop_reason: None,
                    },
                    &metadata,
                )
                .is_err()
        );

        let plan = AgentPlan {
            plan_id: "plan:no-op".to_owned(),
            entries: Vec::new(),
        };
        let planned = running
            .reduce_update(
                &revision('3'),
                &AgentUpdate::Plan {
                    update_id: "update:no-op:plan".to_owned(),
                    plan: plan.clone(),
                },
                &metadata,
            )
            .expect("first Plan change reduces")
            .session;
        assert!(
            planned
                .reduce_update(
                    &revision('4'),
                    &AgentUpdate::Plan {
                        update_id: "update:no-op:plan-again".to_owned(),
                        plan,
                    },
                    &metadata,
                )
                .is_err()
        );

        let usage = Usage {
            used: 1,
            capacity: 2,
            cost: None,
        };
        let metered = planned
            .reduce_update(
                &revision('5'),
                &AgentUpdate::Usage {
                    update_id: "update:no-op:usage".to_owned(),
                    usage: usage.clone(),
                },
                &metadata,
            )
            .expect("first usage change reduces")
            .session;
        assert!(
            metered
                .reduce_update(
                    &revision('6'),
                    &AgentUpdate::Usage {
                        update_id: "update:no-op:usage-again".to_owned(),
                        usage,
                    },
                    &metadata,
                )
                .is_err()
        );
    }

    #[test]
    fn tool_projection_starts_pending_and_never_changes_identity() {
        let session =
            AgentSessionCurrent::new("session:tool-transition").expect("Session constructs");
        let completed = AgentUpdate::Tool {
            update_id: "update:tool:completed-first".to_owned(),
            tool: ToolCall {
                tool_call_id: "tool:one".to_owned(),
                operation: "test.read".to_owned(),
                status: ToolCallStatus::Completed,
                input: serde_json::json!({"path": "README.md"}),
                output: Some(vec![ContentBlock::Text {
                    text: "forged".to_owned(),
                }]),
                locations: Vec::new(),
            },
        };
        assert!(
            session
                .reduce_update(
                    &revision('1'),
                    &completed,
                    &AgentSessionUpdateSource {
                        update: None,
                        entry: AgentSessionEntrySource::Tool { current: None },
                        target_claims: absent_tool_claim("tool:one"),
                    },
                )
                .is_err()
        );

        let pending = AgentUpdate::Tool {
            update_id: "update:tool:pending".to_owned(),
            tool: ToolCall {
                tool_call_id: "tool:one".to_owned(),
                operation: "test.read".to_owned(),
                status: ToolCallStatus::Pending,
                input: serde_json::json!({"path": "README.md"}),
                output: None,
                locations: Vec::new(),
            },
        };
        let pending = session
            .reduce_update(
                &revision('2'),
                &pending,
                &AgentSessionUpdateSource {
                    update: None,
                    entry: AgentSessionEntrySource::Tool { current: None },
                    target_claims: absent_tool_claim("tool:one"),
                },
            )
            .expect("pending Tool enters the Session");
        let AgentSessionUpdateEffect::Tool { current } = pending.effect else {
            panic!("pending Tool produces a typed Tool effect");
        };
        let changed_identity = AgentUpdate::Tool {
            update_id: "update:tool:changed-identity".to_owned(),
            tool: ToolCall {
                tool_call_id: "tool:two".to_owned(),
                operation: "test.read".to_owned(),
                status: ToolCallStatus::AwaitingPermission,
                input: serde_json::json!({"path": "README.md"}),
                output: None,
                locations: Vec::new(),
            },
        };
        assert!(
            pending
                .session
                .reduce_update(
                    &revision('3'),
                    &changed_identity,
                    &AgentSessionUpdateSource {
                        update: None,
                        entry: AgentSessionEntrySource::Tool {
                            current: Some(current),
                        },
                        target_claims: absent_tool_claim("tool:two"),
                    },
                )
                .is_err()
        );
    }

    fn advance_tool_fixture(
        session: &AgentSessionCurrent,
        current: Option<AgentToolCurrent>,
        tool_call_id: &str,
        status: ToolCallStatus,
        command_digit: char,
    ) -> (AgentSessionCurrent, AgentToolCurrent) {
        let update = AgentUpdate::Tool {
            update_id: format!("update:{tool_call_id}:{status:?}"),
            tool: ToolCall {
                tool_call_id: tool_call_id.to_owned(),
                operation: "test.execute".to_owned(),
                status,
                input: serde_json::json!({"tool": tool_call_id}),
                output: status.is_terminal().then(|| {
                    vec![ContentBlock::Text {
                        text: format!("{status:?}"),
                    }]
                }),
                locations: vec!["workspace:fixture".to_owned()],
            },
        };
        let postcondition = session
            .reduce_update(
                &revision(command_digit),
                &update,
                &AgentSessionUpdateSource {
                    update: None,
                    entry: AgentSessionEntrySource::Tool { current },
                    target_claims: absent_tool_claim(tool_call_id),
                },
            )
            .expect("Tool fixture transition reduces");
        let AgentSessionUpdateEffect::Tool { current } = postcondition.effect else {
            panic!("Tool fixture transition returns its exact current")
        };
        (postcondition.session, current)
    }

    fn close_tool_set_fixture() -> (AgentSessionCurrent, Vec<AgentToolCurrent>) {
        let session = AgentSessionCurrent::new("session:close-tools").expect("Session constructs");
        let (session, pending) =
            advance_tool_fixture(&session, None, "tool:pending", ToolCallStatus::Pending, '1');
        let (session, in_progress_pending) = advance_tool_fixture(
            &session,
            None,
            "tool:in-progress",
            ToolCallStatus::Pending,
            '2',
        );
        let (session, in_progress) = advance_tool_fixture(
            &session,
            Some(in_progress_pending),
            "tool:in-progress",
            ToolCallStatus::InProgress,
            '3',
        );
        let (session, awaiting_pending) = advance_tool_fixture(
            &session,
            None,
            "tool:awaiting-permission",
            ToolCallStatus::Pending,
            '4',
        );
        let (session, awaiting_permission) = advance_tool_fixture(
            &session,
            Some(awaiting_pending),
            "tool:awaiting-permission",
            ToolCallStatus::AwaitingPermission,
            '5',
        );

        let mut terminal_session = session;
        for (terminal, digits) in [
            (ToolCallStatus::Completed, ['6', '7', 'a']),
            (ToolCallStatus::Failed, ['8', '9', 'b']),
            (ToolCallStatus::Cancelled, ['a', 'b', 'c']),
        ] {
            let tool_call_id = format!("tool:terminal:{terminal:?}");
            let (session, pending) = advance_tool_fixture(
                &terminal_session,
                None,
                &tool_call_id,
                ToolCallStatus::Pending,
                digits[0],
            );
            let (session, in_progress) = advance_tool_fixture(
                &session,
                Some(pending),
                &tool_call_id,
                ToolCallStatus::InProgress,
                digits[1],
            );
            let (session, current) = advance_tool_fixture(
                &session,
                Some(in_progress),
                &tool_call_id,
                terminal,
                digits[2],
            );
            assert_eq!(current.tool.status, terminal);
            assert!(!session.nonterminal_tools.contains_key(&tool_call_id));
            terminal_session = session;
        }

        (
            terminal_session,
            vec![awaiting_permission, in_progress, pending],
        )
    }

    #[test]
    fn session_close_atomically_cancels_every_nonterminal_tool_and_replays_exactly() {
        let (terminal_session, nonterminal_tools) = close_tool_set_fixture();
        assert_eq!(
            terminal_session
                .nonterminal_tools
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec![
                "tool:awaiting-permission",
                "tool:in-progress",
                "tool:pending"
            ]
        );
        let update = AgentUpdate::State {
            update_id: "update:close-tools".to_owned(),
            state: AgentState::Closed,
            stop_reason: None,
        };
        let command = AgentCommand::new(
            revision('e'),
            AgentCommandAction::SessionUpdate {
                session_id: terminal_session.session_id.clone(),
                update: update.clone(),
            },
        )
        .expect("close command seals");
        let source = AgentSessionUpdateSource {
            update: None,
            target_claims: absent_close_claims(&terminal_session.session_id, &nonterminal_tools),
            entry: AgentSessionEntrySource::Close {
                tools: nonterminal_tools,
            },
        };
        let postcondition = terminal_session
            .reduce_update(&command.command_id, &update, &source)
            .expect("close atomically cancels the complete bounded Tool set");
        assert_eq!(postcondition.session.state, AgentState::Closed);
        assert!(postcondition.session.nonterminal_tools.is_empty());
        let AgentSessionUpdateEffect::Closed { tools } = &postcondition.effect else {
            panic!("Session close returns its explicit Tool terminalization set")
        };
        assert_eq!(
            tools
                .iter()
                .map(|current| (current.tool.tool_call_id.as_str(), current.tool.status))
                .collect::<Vec<_>>(),
            vec![
                ("tool:awaiting-permission", ToolCallStatus::Cancelled),
                ("tool:in-progress", ToolCallStatus::Cancelled),
                ("tool:pending", ToolCallStatus::Cancelled),
            ]
        );
        assert!(tools.iter().all(|current| current.tool.output.is_none()));

        let receipt = AgentCommandReceipt::new(
            &command,
            AgentCommandSource::Session {
                session: terminal_session,
                update: source,
            },
            AgentCommandOutcome::Session(postcondition),
        )
        .expect("close receipt replays the sole close reducer");
        receipt
            .verify_for(&command)
            .expect("close receipt replays exactly");
    }

    #[test]
    fn session_close_rejects_incomplete_reordered_or_nonterminal_source_conflicts() {
        let session =
            AgentSessionCurrent::new("session:close-conflicts").expect("Session constructs");
        let (session, first) =
            advance_tool_fixture(&session, None, "tool:first", ToolCallStatus::Pending, '1');
        let (session, second) =
            advance_tool_fixture(&session, None, "tool:second", ToolCallStatus::Pending, '2');
        let update = AgentUpdate::State {
            update_id: "update:close-conflicts".to_owned(),
            state: AgentState::Closed,
            stop_reason: None,
        };
        for entry in [
            AgentSessionEntrySource::Metadata,
            AgentSessionEntrySource::Close {
                tools: vec![first.clone()],
            },
            AgentSessionEntrySource::Close {
                tools: vec![second.clone(), first.clone()],
            },
        ] {
            assert!(
                session
                    .reduce_update(
                        &revision('3'),
                        &update,
                        &AgentSessionUpdateSource {
                            update: None,
                            entry,
                            target_claims: Vec::new(),
                        },
                    )
                    .is_err()
            );
        }

        let mut mismatched = first.clone();
        mismatched.tool.status = ToolCallStatus::InProgress;
        assert!(
            session
                .reduce_update(
                    &revision('4'),
                    &update,
                    &AgentSessionUpdateSource {
                        update: None,
                        entry: AgentSessionEntrySource::Close {
                            tools: vec![mismatched, second.clone()],
                        },
                        target_claims: Vec::new(),
                    },
                )
                .is_err()
        );

        let mut same_size_different_content = second.clone();
        same_size_different_content.tool.operation = "test.executf".to_owned();
        assert_eq!(
            tool_close_charge(&same_size_different_content).unwrap(),
            tool_close_charge(&second).unwrap()
        );
        assert!(
            session
                .reduce_update(
                    &revision('5'),
                    &update,
                    &AgentSessionUpdateSource {
                        update: None,
                        entry: AgentSessionEntrySource::Close {
                            tools: vec![first, same_size_different_content],
                        },
                        target_claims: Vec::new(),
                    },
                )
                .is_err()
        );
    }

    struct ComposedCloseFixture {
        tool: AgentToolCurrent,
        stream: AgentStreamCurrent,
        prepared: AgentHostOccurrence,
        prepared_postcondition: AgentOccurrencePostcondition,
    }

    fn composed_close_fixture() -> ComposedCloseFixture {
        let session =
            AgentSessionCurrent::new("session:close-composed").expect("Session constructs");
        let (session, pending) = advance_tool_fixture(
            &session,
            None,
            "tool:composed",
            ToolCallStatus::Pending,
            '1',
        );
        let (session, in_progress) = advance_tool_fixture(
            &session,
            Some(pending),
            "tool:composed",
            ToolCallStatus::InProgress,
            '2',
        );
        let open = AgentStreamCommand::Open {
            session_id: session.session_id.clone(),
            stream_id: "stream:composed".to_owned(),
            target: AgentStreamTarget::Tool {
                tool_call_id: in_progress.tool.tool_call_id.clone(),
            },
            delivery: AgentStreamDelivery::Staged,
        };
        let opened = AgentStreamSource::Open {
            session,
            stream: None,
            target: AgentStreamTargetSource::Tool {
                current: Some(in_progress.clone()),
            },
        }
        .reduce(&revision('3'), &open)
        .expect("Tool stream opens");
        let AgentStreamEffect::Opened {
            session: stream_session,
        } = opened.effect
        else {
            panic!("open returns Session metadata")
        };

        let prepared = AgentHostOccurrence::prepare(
            "occurrence:composed",
            "session:close-composed",
            AgentHostRequest::Tool(ToolRequest {
                tool_call_id: "tool:host-composed".to_owned(),
                operation: "test.observe".to_owned(),
                input: serde_json::json!({}),
            }),
            AgentHostBinding::standalone("tool:test/1", "binding:composed/1")
                .expect("binding constructs"),
        )
        .expect("occurrence prepares");
        let prepared_postcondition = AgentOccurrenceSource {
            session: stream_session,
            current: None,
        }
        .reduce(&revision('4'), &prepared)
        .expect("occurrence Prepare reduces");

        ComposedCloseFixture {
            tool: in_progress,
            stream: opened.stream,
            prepared,
            prepared_postcondition,
        }
    }

    fn complete_composed_occurrence(
        prepared: &AgentHostOccurrence,
        prepared_postcondition: AgentOccurrencePostcondition,
    ) -> AgentSessionCurrent {
        let started = prepared.start().expect("occurrence starts");
        let started_postcondition = AgentOccurrenceSource {
            session: prepared_postcondition.session,
            current: Some(prepared_postcondition.current),
        }
        .reduce(&revision('6'), &started)
        .expect("occurrence Started reduces");
        let completed = started
            .complete(AgentHostResponse::Tool(ToolResponse {
                tool_call_id: "tool:host-composed".to_owned(),
                content: vec![ContentBlock::Text {
                    text: "observed".to_owned(),
                }],
                occurrence_binding: "binding:composed/1".to_owned(),
            }))
            .expect("occurrence completes");
        AgentOccurrenceSource {
            session: started_postcondition.session,
            current: Some(started_postcondition.current),
        }
        .reduce(&revision('7'), &completed)
        .expect("occurrence Completed reduces")
        .session
    }

    #[test]
    fn session_close_waits_for_stream_and_occurrence_then_cancels_their_tool_target() {
        let fixture = composed_close_fixture();

        let close = AgentUpdate::State {
            update_id: "update:close-composed".to_owned(),
            state: AgentState::Closed,
            stop_reason: None,
        };
        let close_source = || AgentSessionUpdateSource {
            update: None,
            target_claims: absent_close_claims(
                &fixture.tool.session_id,
                std::slice::from_ref(&fixture.tool),
            ),
            entry: AgentSessionEntrySource::Close {
                tools: vec![fixture.tool.clone()],
            },
        };
        assert!(
            fixture
                .prepared_postcondition
                .session
                .reduce_update(&revision('5'), &close, &close_source())
                .is_err(),
            "open stream and unresolved occurrence both block Session close"
        );

        let completed_session =
            complete_composed_occurrence(&fixture.prepared, fixture.prepared_postcondition);
        assert!(
            completed_session
                .reduce_update(&revision('8'), &close, &close_source())
                .is_err(),
            "open stream independently blocks Session close"
        );

        let abort = AgentStreamCommand::Abort {
            session_id: "session:close-composed".to_owned(),
            stream_id: "stream:composed".to_owned(),
            reason: "Session is closing".to_owned(),
        };
        let aborted = AgentStreamSource::Abort {
            session: completed_session,
            stream: fixture.stream,
            resource: None,
            target_claim: None,
        }
        .reduce(&revision('9'), &abort)
        .expect("stream abort reduces");
        let AgentStreamEffect::Aborted {
            session: ready_to_close,
            resource_release_receipt: None,
        } = aborted.effect
        else {
            panic!("abort returns Session metadata")
        };
        let closed = ready_to_close
            .reduce_update(&revision('a'), &close, &close_source())
            .expect("Session closes after occurrence and stream are terminal");
        let AgentSessionUpdateEffect::Closed { tools } = closed.effect else {
            panic!("close returns Tool cancellations")
        };
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].tool.status, ToolCallStatus::Cancelled);
    }

    #[test]
    fn nonterminal_tool_directory_enforces_count_and_close_byte_reservation() {
        let mut too_many =
            AgentSessionCurrent::new("session:tool-directory-count").expect("Session constructs");
        for index in 0..=MAX_AGENT_NONTERMINAL_TOOLS {
            too_many.nonterminal_tools.insert(
                format!("tool:{index:03}"),
                AgentNonterminalTool {
                    current_digest: "1".repeat(64),
                    close_bytes: 1,
                },
            );
        }
        assert!(too_many.verify().is_err());

        let mut too_large =
            AgentSessionCurrent::new("session:tool-directory-bytes").expect("Session constructs");
        too_large.nonterminal_tools.insert(
            "tool:oversized-close".to_owned(),
            AgentNonterminalTool {
                current_digest: "2".repeat(64),
                close_bytes: MAX_AGENT_TOOL_CLOSE_BYTES as u64 + 1,
            },
        );
        assert!(too_large.verify().is_err());
    }

    fn commit_fixture() -> (AgentCommand, AgentCommandReceipt) {
        let update = AgentUpdate::State {
            update_id: "update:receipt:no-cycle".to_owned(),
            state: AgentState::Running,
            stop_reason: None,
        };
        let command = AgentCommand::new(
            revision('1'),
            AgentCommandAction::SessionUpdate {
                session_id: "session:receipt:no-cycle".to_owned(),
                update: update.clone(),
            },
        )
        .expect("command seals");
        let session =
            AgentSessionCurrent::new("session:receipt:no-cycle").expect("Session constructs");
        let source = AgentSessionUpdateSource {
            update: None,
            entry: AgentSessionEntrySource::Metadata,
            target_claims: Vec::new(),
        };
        let outcome = session
            .reduce_update(&command.command_id, &update, &source)
            .expect("update reduces");
        let receipt = AgentCommandReceipt::new(
            &command,
            AgentCommandSource::Session {
                session,
                update: source,
            },
            AgentCommandOutcome::Session(outcome),
        )
        .expect("semantic receipt seals");
        (command, receipt)
    }

    #[test]
    fn semantic_receipt_has_no_result_root_self_reference() {
        let (command, receipt) = commit_fixture();
        let encoded = serde_json::to_value(&receipt).expect("receipt encodes");
        assert!(encoded.get("revision").is_none());
        assert!(encoded.get("result_revision").is_none());
        assert!(encoded.get("committed_revision").is_none());

        let first = AgentCommit {
            observed_revision: revision('2'),
            committed_revision: Some(revision('2')),
            receipt: receipt.clone(),
        };
        let replay = AgentCommit {
            observed_revision: revision('3'),
            committed_revision: None,
            receipt: receipt.clone(),
        };
        first
            .verify_for(&command)
            .expect("commit envelope verifies");
        replay
            .verify_for(&command)
            .expect("replay envelope verifies");
        assert_eq!(first.receipt, replay.receipt);
        assert_ne!(first.observed_revision, replay.observed_revision);

        let mut forbidden = encoded;
        forbidden
            .as_object_mut()
            .expect("receipt fixture is an object")
            .insert("result_revision".to_owned(), Value::String(revision('2')));
        assert!(serde_json::from_value::<AgentCommandReceipt>(forbidden).is_err());
        let malformed = AgentCommit {
            observed_revision: "sha256:not-a-root".to_owned(),
            committed_revision: None,
            receipt,
        };
        assert!(malformed.verify_for(&command).is_err());
    }

    #[test]
    fn commit_acknowledgement_is_required_nullable_and_distinct_from_same_head_replay() {
        let (command, receipt) = commit_fixture();
        let fresh = AgentCommit {
            observed_revision: revision('2'),
            committed_revision: Some(revision('2')),
            receipt,
        };
        let replay = AgentCommit {
            committed_revision: None,
            ..fresh.clone()
        };
        assert_eq!(fresh.observed_revision, replay.observed_revision);
        assert_eq!(fresh.receipt, replay.receipt);
        assert_ne!(fresh, replay);

        for commit in [fresh, replay] {
            let encoded = serde_json::to_vec(&commit).expect("commit encodes");
            let decoded: AgentCommit =
                cymule_core::decode_json(&encoded).expect("explicit nullable commit decodes");
            decoded.verify_for(&command).expect("commit verifies");
            assert_eq!(decoded, commit);
            let wire = serde_json::to_value(&commit).expect("commit value encodes");
            assert_eq!(
                wire.get("committed_revision"),
                Some(&serde_json::to_value(&commit.committed_revision).unwrap()),
            );
            let mut missing = wire.clone();
            missing
                .as_object_mut()
                .unwrap()
                .remove("committed_revision");
            assert!(serde_json::from_value::<AgentCommit>(missing).is_err());
            let mut wrong_type = wire.clone();
            wrong_type["committed_revision"] = Value::Bool(true);
            assert!(serde_json::from_value::<AgentCommit>(wrong_type).is_err());
            let mut unknown = wire;
            unknown["fresh"] = Value::Bool(true);
            assert!(serde_json::from_value::<AgentCommit>(unknown).is_err());
        }
    }

    #[test]
    fn commit_rejects_malformed_acknowledgements_and_tampered_receipts() {
        let (command, receipt) = commit_fixture();
        let fresh = AgentCommit {
            observed_revision: revision('2'),
            committed_revision: Some(revision('2')),
            receipt,
        };
        let mut wrong_revision = fresh.clone();
        wrong_revision.committed_revision = Some(revision('3'));
        assert!(matches!(
            wrong_revision.verify_for(&command),
            Err(ProtocolError::IdentityMismatch(_))
        ));
        for malformed in ["sha256:unsafe", "", "sha256:AAAAAAAA"] {
            let mut malformed_acknowledgement = fresh.clone();
            malformed_acknowledgement.committed_revision = Some(malformed.to_owned());
            assert!(malformed_acknowledgement.verify_for(&command).is_err());
        }
        let mut wrong_receipt = fresh.clone();
        wrong_receipt.receipt.command_id = revision('4');
        assert!(wrong_receipt.verify_for(&command).is_err());
        let mut wrong_outcome = fresh;
        let AgentCommandOutcome::Session(outcome) = &mut wrong_outcome.receipt.outcome else {
            panic!("fixture has a Session outcome");
        };
        outcome.session.state = AgentState::Idle;
        assert!(wrong_outcome.verify_for(&command).is_err());
    }

    #[test]
    fn occurrence_rejects_new_command_noop_closed_session_and_m1_workspace_escape() {
        let binding = AgentHostBinding::standalone("context:test/1", "binding:context/1")
            .expect("binding constructs");
        let occurrence = AgentHostOccurrence::prepare(
            "occurrence:one",
            "session:occurrence",
            AgentHostRequest::Context(context_request("session:occurrence")),
            binding.clone(),
        )
        .expect("occurrence prepares");
        let source = AgentOccurrenceSource {
            session: AgentSessionCurrent::new("session:occurrence")
                .expect("Session current constructs"),
            current: None,
        };
        let first = source
            .reduce(&revision('1'), &occurrence)
            .expect("first Prepare reduces");
        assert!(
            AgentOccurrenceSource {
                session: first.session.clone(),
                current: Some(first.current.clone()),
            }
            .reduce(&revision('2'), &occurrence)
            .is_err()
        );
        let mut closed =
            AgentSessionCurrent::new("session:occurrence").expect("Session constructs");
        closed.state = AgentState::Closed;
        assert!(
            AgentOccurrenceSource {
                session: closed,
                current: None,
            }
            .reduce(&revision('3'), &occurrence)
            .is_err()
        );
        let mut advanced_session =
            AgentSessionCurrent::new("session:occurrence").expect("Session constructs");
        advanced_session.message_count = 1;
        advanced_session.message_head = Some(revision('9'));
        advanced_session
            .verify()
            .expect("advanced Session metadata verifies");
        assert!(matches!(
            AgentOccurrenceSource {
                session: advanced_session,
                current: None,
            }
            .reduce(&revision('5'), &occurrence),
            Err(ProtocolError::IdentityMismatch(_))
        ));

        let overlay = cymule_core::artifact_ref("test.workspace-overlay/1", b"overlay")
            .expect("overlay Artifact derives");
        let request = WorkspaceHostRequest::m1_scope(
            WorkspaceOccurrenceOwner {
                run_id: "run:workspace".to_owned(),
                scope_id: "scope:workspace".to_owned(),
                invocation_id: "invocation:workspace".to_owned(),
                site_id: "site:workspace".to_owned(),
                occurrence_key: "occurrence-key:workspace".to_owned(),
                operation: "workspace.commit".to_owned(),
                effect_intent_id: Some("effect:workspace".to_owned()),
            },
            WorkspaceChange {
                change_id: "change:workspace".to_owned(),
                overlay,
                commit: true,
            },
        )
        .expect("M1 workspace request constructs");
        let workspace_occurrence = AgentHostOccurrence::prepare(
            "occurrence:workspace",
            "session:workspace",
            AgentHostRequest::Workspace(request),
            binding,
        )
        .expect("workspace occurrence prepares");
        assert!(
            AgentCommand::new(
                revision('4'),
                AgentCommandAction::Occurrence {
                    occurrence: Box::new(workspace_occurrence),
                },
            )
            .is_err()
        );
    }

    #[test]
    fn context_source_descriptor_is_required_safe_and_prefix_bounded() {
        let request = context_request("session:context-descriptor");
        AgentHostRequest::Context(request.clone())
            .validate_for_session("session:context-descriptor")
            .expect("empty Context source descriptor verifies");
        let request_wire = serde_json::to_value(&request).expect("Context request encodes");
        for invalid in [None, Some(Value::Null), Some(Value::String("0".to_owned()))] {
            let mut wire = request_wire.clone();
            match invalid {
                None => {
                    wire.as_object_mut()
                        .expect("Context request is an object")
                        .remove("source_message_count");
                }
                Some(value) => wire["source_message_count"] = value,
            }
            assert!(serde_json::from_value::<ContextRequest>(wire).is_err());
        }

        let mut wrong = request.clone();
        wrong.source_message_head = Some(revision('1'));
        assert!(
            AgentHostRequest::Context(wrong)
                .validate_for_session("session:context-descriptor")
                .is_err()
        );
        let mut wrong = request.clone();
        wrong.source_message_count = 1;
        assert!(
            AgentHostRequest::Context(wrong)
                .validate_for_session("session:context-descriptor")
                .is_err()
        );
        let mut wrong = request;
        wrong.source_message_count = MAX_EXACT_INTEGER + 1;
        wrong.source_message_head = Some(revision('1'));
        assert!(
            AgentHostRequest::Context(wrong)
                .validate_for_session("session:context-descriptor")
                .is_err()
        );

        let snapshot = ContextSnapshot {
            snapshot_id: "snapshot:context-descriptor".to_owned(),
            source_message_head: Some(revision('2')),
            source_message_count: 1,
            selected_messages: vec![AgentContextMessageRef {
                index: 0,
                message_id: "message:context-descriptor".to_owned(),
                message_digest: revision('3'),
            }],
            content: Vec::new(),
            occurrence_binding: "binding:context-descriptor/1".to_owned(),
        };
        validate_context_snapshot(&snapshot).expect("bounded Context snapshot verifies");
        let snapshot_wire = serde_json::to_value(&snapshot).expect("Context snapshot encodes");
        for invalid in [None, Some(Value::Null), Some(Value::String("1".to_owned()))] {
            let mut wire = snapshot_wire.clone();
            match invalid {
                None => {
                    wire.as_object_mut()
                        .expect("Context snapshot is an object")
                        .remove("source_message_count");
                }
                Some(value) => wire["source_message_count"] = value,
            }
            assert!(serde_json::from_value::<ContextSnapshot>(wire).is_err());
        }
        let mut outside_prefix = snapshot;
        outside_prefix.selected_messages[0].index = outside_prefix.source_message_count;
        assert!(validate_context_snapshot(&outside_prefix).is_err());
    }

    #[test]
    fn initial_context_and_model_occurrences_match_session_head_and_count() {
        let session_id = "session:occurrence-prefix";
        let head = revision('7');
        let context = ContextRequest {
            session_id: session_id.to_owned(),
            source_message_head: Some(head.clone()),
            source_message_count: 1,
            budget: 1,
            scan_limits: AgentContextScanLimits {
                max_entries: 1,
                max_canonical_bytes: 1024,
            },
        };
        let binding = AgentHostBinding::standalone("context:test/1", "binding:context-prefix/1")
            .expect("Context binding constructs");
        let context_occurrence = AgentHostOccurrence::prepare(
            "occurrence:context-prefix",
            session_id,
            AgentHostRequest::Context(context),
            binding,
        )
        .expect("Context occurrence prepares");
        let model_occurrence = AgentHostOccurrence::prepare(
            "occurrence:model-prefix",
            session_id,
            AgentHostRequest::Model(ModelRequest {
                session_id: session_id.to_owned(),
                context: ContextSnapshot {
                    snapshot_id: "snapshot:model-prefix".to_owned(),
                    source_message_head: Some(head.clone()),
                    source_message_count: 1,
                    selected_messages: Vec::new(),
                    content: Vec::new(),
                    occurrence_binding: "binding:context-prefix/1".to_owned(),
                },
                tools: Vec::new(),
            }),
            AgentHostBinding::standalone("model:test/1", "binding:model-prefix/1")
                .expect("Model binding constructs"),
        )
        .expect("Model occurrence prepares");

        let mut wrong_count = AgentSessionCurrent::new(session_id).expect("Session constructs");
        wrong_count.message_head = Some(head.clone());
        wrong_count.message_count = 2;
        wrong_count
            .verify()
            .expect("independent Session current verifies");
        for occurrence in [&context_occurrence, &model_occurrence] {
            assert!(matches!(
                AgentOccurrenceSource {
                    session: wrong_count.clone(),
                    current: None,
                }
                .reduce(&revision('1'), occurrence),
                Err(ProtocolError::IdentityMismatch(_))
            ));
        }

        let mut admitted_session = wrong_count;
        admitted_session.message_count = 1;
        let prepared = AgentOccurrenceSource {
            session: admitted_session,
            current: None,
        }
        .reduce(&revision('2'), &context_occurrence)
        .expect("matching head and count admit the Context occurrence");
        let mut later_session = prepared.session;
        later_session.message_count = 2;
        later_session.message_head = Some(revision('8'));
        later_session
            .verify()
            .expect("Session may append after occurrence admission");
        AgentOccurrenceSource {
            session: later_session,
            current: Some(prepared.current),
        }
        .reduce(
            &revision('3'),
            &context_occurrence
                .start()
                .expect("Context occurrence starts"),
        )
        .expect("later lifecycle uses the immutable admitted request, not the new Session tip");
    }

    #[test]
    fn occurrence_transition_identity_binds_the_complete_terminal_response() {
        let binding = AgentHostBinding::standalone("tool:test/1", "binding:tool/1")
            .expect("tool binding constructs");
        let prepared = AgentHostOccurrence::prepare(
            "occurrence:transition-id",
            "session:transition-id",
            AgentHostRequest::Tool(ToolRequest {
                tool_call_id: "tool:transition-id".to_owned(),
                operation: "test.read".to_owned(),
                input: serde_json::json!({"path": "README.md"}),
            }),
            binding,
        )
        .expect("tool occurrence prepares");
        let started = prepared.start().expect("tool occurrence starts");
        let completed_a = started
            .complete(AgentHostResponse::Tool(ToolResponse {
                tool_call_id: "tool:transition-id".to_owned(),
                content: vec![ContentBlock::Text {
                    text: "first".to_owned(),
                }],
                occurrence_binding: "binding:tool/1".to_owned(),
            }))
            .expect("first terminal response validates");
        let completed_b = started
            .complete(AgentHostResponse::Tool(ToolResponse {
                tool_call_id: "tool:transition-id".to_owned(),
                content: vec![ContentBlock::Text {
                    text: "second".to_owned(),
                }],
                occurrence_binding: "binding:tool/1".to_owned(),
            }))
            .expect("second terminal response validates independently");

        assert_eq!(
            completed_a
                .transition_id()
                .expect("transition identity derives deterministically"),
            completed_a
                .transition_id()
                .expect("transition identity re-derives")
        );
        assert_ne!(
            completed_a
                .transition_id()
                .expect("first transition identity derives"),
            completed_b
                .transition_id()
                .expect("second transition identity derives")
        );
    }

    #[test]
    fn unknown_reconciliation_observations_are_append_only_idempotent_and_reopenable() {
        let binding = AgentHostBinding::standalone("tool:test/1", "binding:observation/1")
            .expect("observation binding constructs");
        let started = AgentHostOccurrence::prepare(
            "occurrence:observation",
            "session:observation",
            AgentHostRequest::Tool(ToolRequest {
                tool_call_id: "tool:observation".to_owned(),
                operation: "test.observe".to_owned(),
                input: serde_json::json!({}),
            }),
            binding,
        )
        .expect("observation occurrence prepares")
        .start()
        .expect("observation occurrence starts");
        let unknown = started
            .mark_unknown("dispatch response was lost")
            .expect("started occurrence becomes unknown");
        let first_evidence = vec![ContentBlock::Text {
            text: "provider has no terminal receipt".to_owned(),
        }];
        let first = unknown
            .mark_unknown_with_evidence("ignored replacement", first_evidence.clone())
            .expect("first observation appends");
        let duplicate = first
            .mark_unknown_with_evidence("ignored replacement", first_evidence)
            .expect("exact duplicate observation is idempotent");
        assert_eq!(duplicate, first);

        let second = first
            .mark_unknown_with_evidence(
                "ignored replacement",
                vec![ContentBlock::Text {
                    text: "readback remains inconclusive".to_owned(),
                }],
            )
            .expect("new observation appends");
        assert_eq!(second.failure, unknown.failure);
        assert_eq!(second.recovery_observations.len(), 2);
        assert_eq!(
            second.recovery_observations[0],
            first.recovery_observations[0]
        );

        let bytes = cymule_core::canonical_bytes(&second).expect("occurrence encodes");
        let reopened: AgentHostOccurrence =
            cymule_core::decode_json(&bytes).expect("occurrence reopens through strict JSON");
        assert_eq!(reopened, second);
        reopened.validate().expect("reopened observations verify");

        let not_applied = reopened
            .mark_not_applied(vec![ContentBlock::Text {
                text: "provider proves no write occurred".to_owned(),
            }])
            .expect("terminal not-applied evidence appends");
        assert_eq!(not_applied.recovery_observations.len(), 3);
        assert_eq!(
            not_applied
                .recovery_observations
                .last()
                .map(|item| item.disposition),
            Some(AgentRecoveryObservationDisposition::NotApplied)
        );
    }

    #[test]
    fn recovery_observation_accumulator_enforces_its_exact_count_bound() {
        let binding = AgentHostBinding::standalone("tool:test/1", "binding:max-observation/1")
            .expect("observation binding constructs");
        let mut current = AgentHostOccurrence::prepare(
            "occurrence:max-observation",
            "session:max-observation",
            AgentHostRequest::Tool(ToolRequest {
                tool_call_id: "tool:max-observation".to_owned(),
                operation: "test.observe".to_owned(),
                input: serde_json::json!({}),
            }),
            binding,
        )
        .expect("observation occurrence prepares")
        .start()
        .expect("observation occurrence starts")
        .mark_unknown("dispatch response was lost")
        .expect("occurrence becomes unknown");
        for index in 0..MAX_AGENT_RECOVERY_OBSERVATIONS - 1 {
            current = current
                .mark_unknown_with_evidence(
                    "ignored replacement",
                    vec![ContentBlock::Text {
                        text: format!("observation {index}"),
                    }],
                )
                .expect("observation within the exact bound appends");
        }
        assert_eq!(
            current.recovery_observations.len(),
            MAX_AGENT_RECOVERY_OBSERVATIONS - 1
        );
        assert!(
            current
                .mark_unknown_with_evidence(
                    "ignored replacement",
                    vec![ContentBlock::Text {
                        text: "one observation too many".to_owned(),
                    }],
                )
                .is_err()
        );
        let terminal = current
            .mark_not_applied(vec![ContentBlock::Text {
                text: "terminal provider absence proof".to_owned(),
            }])
            .expect("reserved terminal observation slot remains available");
        assert_eq!(
            terminal.recovery_observations.len(),
            MAX_AGENT_RECOVERY_OBSERVATIONS
        );
    }

    fn reduce_occurrence_snapshot(
        source: &AgentOccurrenceSource,
        occurrence: &AgentHostOccurrence,
    ) -> (AgentOccurrenceSource, AgentCommand, AgentCommandReceipt) {
        let command = AgentCommand::new(
            revision('1'),
            AgentCommandAction::Occurrence {
                occurrence: Box::new(occurrence.clone()),
            },
        )
        .expect("public occurrence command seals");
        let postcondition = source
            .reduce(&command.command_id, occurrence)
            .expect("public occurrence snapshot reduces");
        let receipt = AgentCommandReceipt::new(
            &command,
            AgentCommandSource::Occurrence(source.clone()),
            AgentCommandOutcome::Occurrence(postcondition.clone()),
        )
        .expect("public occurrence receipt verifies");
        (
            AgentOccurrenceSource {
                session: postcondition.session,
                current: Some(postcondition.current),
            },
            command,
            receipt,
        )
    }

    fn occurrence_source_at_recovery_limit() -> AgentOccurrenceSource {
        let prepared = AgentHostOccurrence::prepare(
            "occurrence:direct-bound",
            "session:direct-bound",
            AgentHostRequest::Tool(ToolRequest {
                tool_call_id: "tool:direct-bound".to_owned(),
                operation: "test.observe".to_owned(),
                input: serde_json::json!({}),
            }),
            AgentHostBinding::standalone("tool:test/1", "binding:direct-bound/1")
                .expect("binding constructs"),
        )
        .expect("occurrence prepares");
        let source = AgentOccurrenceSource {
            session: AgentSessionCurrent::new("session:direct-bound").expect("Session constructs"),
            current: None,
        };
        let (source, _, _) = reduce_occurrence_snapshot(&source, &prepared);
        let started = prepared.start().expect("occurrence starts");
        let (source, _, _) = reduce_occurrence_snapshot(&source, &started);
        let mut unknown = started
            .mark_unknown("dispatch acknowledgement lost")
            .expect("occurrence becomes Unknown");
        for index in 0..MAX_AGENT_RECOVERY_OBSERVATIONS - 1 {
            unknown = unknown
                .mark_unknown_with_evidence(
                    "ignored replacement",
                    vec![ContentBlock::Text {
                        text: format!("unknown observation {index}"),
                    }],
                )
                .expect("bounded observation appends");
        }
        reduce_occurrence_snapshot(&source, &unknown).0
    }

    #[test]
    fn occurrence_snapshot_reserves_terminal_observation_capacity() {
        let source = occurrence_source_at_recovery_limit();
        let prior = source.current.as_ref().expect("Unknown current exists");
        let mut overflow = prior.occurrence.clone();
        overflow.recovery_observations.push(
            AgentRecoveryObservation::new(
                &overflow.occurrence_id,
                AgentRecoveryObservationDisposition::Unknown,
                vec![ContentBlock::Text {
                    text: "one more unknown observation".to_owned(),
                }],
            )
            .expect("individual overflow observation is valid"),
        );
        assert_eq!(
            overflow.recovery_observations.len(),
            MAX_AGENT_RECOVERY_OBSERVATIONS
        );
        assert!(
            AgentCommand::new(
                revision('2'),
                AgentCommandAction::Occurrence {
                    occurrence: Box::new(overflow.clone()),
                },
            )
            .is_err(),
            "public occurrence command must reject the 64th Unknown observation"
        );
        assert!(overflow.validate().is_err());
        assert!(source.reduce(&revision('2'), &overflow).is_err());

        let terminal = prior
            .occurrence
            .mark_not_applied(vec![ContentBlock::Text {
                text: "terminal provider absence proof".to_owned(),
            }])
            .expect("reserved terminal slot remains available");
        assert_eq!(
            terminal.recovery_observations.len(),
            MAX_AGENT_RECOVERY_OBSERVATIONS
        );
        let (after, command, mut receipt) = reduce_occurrence_snapshot(&source, &terminal);
        assert_eq!(after.session.unresolved_occurrence_count, 0);
        let reopened: AgentCommandReceipt = cymule_core::decode_json(
            &cymule_core::canonical_bytes(&receipt).expect("terminal receipt encodes"),
        )
        .expect("terminal receipt reopens");
        reopened
            .verify_for(&command)
            .expect("reopened terminal receipt verifies");
        let AgentCommandSource::Occurrence(receipt_source) = &mut receipt.source else {
            panic!("receipt retains its occurrence source")
        };
        receipt_source
            .current
            .as_mut()
            .expect("receipt retains the Unknown current")
            .occurrence = overflow;
        receipt.receipt_id =
            agent_command_receipt_id(&receipt.command_id, &receipt.source, &receipt.outcome)
                .expect("tampered receipt identity reseals");
        assert!(receipt.verify_for(&command).is_err());
        prior
            .verify()
            .expect("rejected snapshot preserves valid source");
    }

    #[test]
    fn stream_tracks_open_count_and_rejects_closed_or_existing_message_targets() {
        let stream = AgentStreamCommand::Open {
            session_id: "session:stream".to_owned(),
            stream_id: "stream:one".to_owned(),
            target: AgentStreamTarget::Message {
                message_id: "message:stream".to_owned(),
                role: MessageRole::Agent,
            },
            delivery: AgentStreamDelivery::Staged,
        };
        let command = AgentCommand::new(revision('1'), AgentCommandAction::Stream(stream.clone()))
            .expect("stream command seals");
        let session = AgentSessionCurrent::new("session:stream").expect("Session constructs");
        let opened = AgentStreamSource::Open {
            session: session.clone(),
            stream: None,
            target: AgentStreamTargetSource::Message { current: None },
        }
        .reduce(&command.command_id, &stream)
        .expect("stream opens");
        let AgentStreamEffect::Opened {
            session: open_session,
        } = &opened.effect
        else {
            panic!("open outcome shape")
        };
        assert_eq!(open_session.open_stream_count, 1);

        let abort = AgentStreamCommand::Abort {
            session_id: "session:stream".to_owned(),
            stream_id: "stream:one".to_owned(),
            reason: "caller:abort".to_owned(),
        };
        let abort_command =
            AgentCommand::new(revision('2'), AgentCommandAction::Stream(abort.clone()))
                .expect("abort command seals");
        let aborted = AgentStreamSource::Abort {
            session: open_session.clone(),
            stream: opened.stream,
            resource: None,
            target_claim: None,
        }
        .reduce(&abort_command.command_id, &abort)
        .expect("stream aborts");
        let AgentStreamEffect::Aborted {
            session: aborted_session,
            resource_release_receipt: None,
        } = aborted.effect
        else {
            panic!("abort outcome shape")
        };
        assert_eq!(aborted_session.open_stream_count, 0);

        let mut closed = session;
        closed.state = AgentState::Closed;
        assert!(
            AgentStreamSource::Open {
                session: closed,
                stream: None,
                target: AgentStreamTargetSource::Message { current: None },
            }
            .reduce(&command.command_id, &stream)
            .is_err()
        );

        let message = message_update(
            "update:message:existing",
            "message:stream",
            "existing".to_owned(),
        );
        let message_command = revision('3');
        let message_post = AgentSessionCurrent::new("session:stream")
            .expect("Session constructs")
            .reduce_update(
                &message_command,
                &message,
                &AgentSessionUpdateSource {
                    update: None,
                    entry: AgentSessionEntrySource::Message { current: None },
                    target_claims: absent_message_claim("message:stream"),
                },
            )
            .expect("message admits");
        let AgentSessionUpdateEffect::Message { current } = message_post.effect else {
            panic!("message effect shape")
        };
        assert!(
            AgentStreamSource::Open {
                session: message_post.session,
                stream: None,
                target: AgentStreamTargetSource::Message {
                    current: Some(current),
                },
            }
            .reduce(&command.command_id, &stream)
            .is_err()
        );
    }

    #[test]
    fn staged_stream_enforces_count_and_keeps_receipt_bounded() {
        let open = AgentStreamCommand::Open {
            session_id: "session:bounded-stream".to_owned(),
            stream_id: "stream:bounded-stream".to_owned(),
            target: AgentStreamTarget::Message {
                message_id: "message:bounded-stream".to_owned(),
                role: MessageRole::Agent,
            },
            delivery: AgentStreamDelivery::Staged,
        };
        let (session, mut stream) = open_message_stream(&open, '1');
        let mut chunks = Vec::new();

        for sequence in 0..MAX_AGENT_STREAM_CHUNKS as u64 {
            let append = AgentStreamCommand::AppendChunk {
                session_id: "session:bounded-stream".to_owned(),
                stream_id: "stream:bounded-stream".to_owned(),
                chunk: AgentStreamChunk {
                    sequence,
                    content: vec![ContentBlock::Text {
                        text: "x".repeat(3 * 1024),
                    }],
                },
            };
            let command =
                AgentCommand::new(revision('2'), AgentCommandAction::Stream(append.clone()))
                    .expect("append command seals");
            let postcondition = AgentStreamSource::AppendChunk {
                stream: stream.clone(),
                current_chunk: None,
            }
            .reduce(&command.command_id, &append)
            .expect("bounded chunk appends");
            let AgentStreamEffect::Chunk { current } = postcondition.effect else {
                panic!("chunk effect shape")
            };
            chunks.push(current);
            stream = postcondition.stream;
        }
        assert_eq!(stream.next_chunk_sequence, MAX_AGENT_STREAM_CHUNKS as u64);
        assert!(
            AgentStreamSource::AppendChunk {
                stream: stream.clone(),
                current_chunk: None,
            }
            .reduce(
                &revision('3'),
                &AgentStreamCommand::AppendChunk {
                    session_id: "session:bounded-stream".to_owned(),
                    stream_id: "stream:bounded-stream".to_owned(),
                    chunk: AgentStreamChunk {
                        sequence: MAX_AGENT_STREAM_CHUNKS as u64,
                        content: vec![ContentBlock::Text {
                            text: "overflow".to_owned(),
                        }],
                    },
                },
            )
            .is_err()
        );

        let finalize = AgentStreamCommand::Finalize {
            session_id: "session:bounded-stream".to_owned(),
            stream_id: "stream:bounded-stream".to_owned(),
        };
        let finalize_command =
            AgentCommand::new(revision('4'), AgentCommandAction::Stream(finalize.clone()))
                .expect("finalize command seals");
        let source = AgentStreamSource::Finalize {
            session,
            stream,
            chunks,
            target: AgentStreamTargetSource::Message { current: None },
            update: None,
            resource: None,
            target_claim: None,
        };
        let postcondition = source
            .reduce(&finalize_command.command_id, &finalize)
            .expect("max-count staged stream finalizes");
        let receipt = AgentCommandReceipt::new(
            &finalize_command,
            AgentCommandSource::Stream(Box::new(source)),
            AgentCommandOutcome::Stream(postcondition),
        )
        .expect("bounded finalize receipt seals");
        assert!(
            cymule_core::canonical_bytes(&receipt)
                .expect("receipt encodes canonically")
                .len()
                <= MAX_AGENT_RECEIPT_BYTES
        );
    }

    #[test]
    fn staged_stream_enforces_cumulative_byte_bound() {
        let byte_open = AgentStreamCommand::Open {
            session_id: "session:byte-stream".to_owned(),
            stream_id: "stream:byte-stream".to_owned(),
            target: AgentStreamTarget::Message {
                message_id: "message:byte-stream".to_owned(),
                role: MessageRole::Agent,
            },
            delivery: AgentStreamDelivery::Staged,
        };
        let (_, byte_stream) = open_message_stream(&byte_open, '5');
        let first = AgentStreamCommand::AppendChunk {
            session_id: "session:byte-stream".to_owned(),
            stream_id: "stream:byte-stream".to_owned(),
            chunk: AgentStreamChunk {
                sequence: 0,
                content: vec![ContentBlock::Text {
                    text: "a".repeat(150 * 1024),
                }],
            },
        };
        let first_command =
            AgentCommand::new(revision('6'), AgentCommandAction::Stream(first.clone()))
                .expect("first byte-bound append seals");
        let first = AgentStreamSource::AppendChunk {
            stream: byte_stream,
            current_chunk: None,
        }
        .reduce(&first_command.command_id, &first)
        .expect("first byte-bound chunk appends");
        let second = AgentStreamCommand::AppendChunk {
            session_id: "session:byte-stream".to_owned(),
            stream_id: "stream:byte-stream".to_owned(),
            chunk: AgentStreamChunk {
                sequence: 1,
                content: vec![ContentBlock::Text {
                    text: "b".repeat(150 * 1024),
                }],
            },
        };
        let second_command =
            AgentCommand::new(revision('7'), AgentCommandAction::Stream(second.clone()))
                .expect("second byte-bound append seals independently");
        assert!(
            AgentStreamSource::AppendChunk {
                stream: first.stream,
                current_chunk: None,
            }
            .reduce(&second_command.command_id, &second)
            .is_err()
        );
    }

    fn append_staged_content(
        stream: AgentStreamCurrent,
        count: usize,
    ) -> (AgentStreamCurrent, AgentStreamChunkCurrent) {
        append_staged_chunk(
            stream,
            vec![
                ContentBlock::Text {
                    text: "x".to_owned()
                };
                count
            ],
        )
        .expect("content through the exact bound appends")
    }

    fn append_staged_chunk(
        stream: AgentStreamCurrent,
        content: Vec<ContentBlock>,
    ) -> ProtocolResult<(AgentStreamCurrent, AgentStreamChunkCurrent)> {
        let append = AgentStreamCommand::AppendChunk {
            session_id: stream.session_id.clone(),
            stream_id: stream.stream_id.clone(),
            chunk: AgentStreamChunk {
                sequence: stream.next_chunk_sequence,
                content,
            },
        };
        let command = AgentCommand::new(revision('2'), AgentCommandAction::Stream(append.clone()))?;
        let source = AgentStreamSource::AppendChunk {
            stream,
            current_chunk: None,
        };
        let postcondition = source.reduce(&command.command_id, &append)?;
        AgentCommandReceipt::new(
            &command,
            AgentCommandSource::Stream(Box::new(source)),
            AgentCommandOutcome::Stream(postcondition.clone()),
        )?;
        let AgentStreamEffect::Chunk { current } = postcondition.effect else {
            panic!("append returns its immutable chunk")
        };
        Ok((postcondition.stream, current))
    }

    fn stream_capacity_fixture(
        tool_target: bool,
    ) -> (
        AgentSessionCurrent,
        AgentStreamCurrent,
        AgentStreamTargetSource,
    ) {
        let mut session = AgentSessionCurrent::new("session:wrapper-capacity")
            .expect("capacity Session constructs");
        let target_id = "🧪".repeat(512);
        let (target, target_source) = if tool_target {
            let mut current = None;
            for status in [ToolCallStatus::Pending, ToolCallStatus::InProgress] {
                let update = AgentUpdate::Tool {
                    update_id: format!("update:capacity:{status:?}"),
                    tool: ToolCall {
                        tool_call_id: target_id.clone(),
                        operation: "workspace.read".to_owned(),
                        status,
                        input: serde_json::json!({"retained": "i".repeat(16 * 1024)}),
                        output: None,
                        locations: vec!["source:large-input".to_owned()],
                    },
                };
                let postcondition = session
                    .reduce_update(
                        &revision('1'),
                        &update,
                        &AgentSessionUpdateSource {
                            update: None,
                            entry: AgentSessionEntrySource::Tool { current },
                            target_claims: absent_tool_claim(&target_id),
                        },
                    )
                    .expect("legal Tool lifecycle advances");
                let AgentSessionUpdateEffect::Tool { current: next } = postcondition.effect else {
                    panic!("Tool update returns its exact current")
                };
                current = Some(next);
                session = postcondition.session;
            }
            (
                AgentStreamTarget::Tool {
                    tool_call_id: target_id,
                },
                AgentStreamTargetSource::Tool { current },
            )
        } else {
            (
                AgentStreamTarget::Message {
                    message_id: target_id,
                    role: MessageRole::Agent,
                },
                AgentStreamTargetSource::Message { current: None },
            )
        };
        let open = AgentStreamCommand::Open {
            session_id: session.session_id.clone(),
            stream_id: "stream:wrapper-capacity".to_owned(),
            target,
            delivery: AgentStreamDelivery::Staged,
        };
        let command = AgentCommand::new(revision('2'), AgentCommandAction::Stream(open.clone()))
            .expect("capacity stream Open seals");
        let postcondition = AgentStreamSource::Open {
            session,
            stream: None,
            target: target_source.clone(),
        }
        .reduce(&command.command_id, &open)
        .expect("capacity stream opens");
        let AgentStreamEffect::Opened { session } = postcondition.effect else {
            panic!("Open returns its exact Session")
        };
        (session, postcondition.stream, target_source)
    }

    fn finalized_staged_stream_current(tool_target: bool) -> AgentStreamCurrent {
        let (session, stream, target) = stream_capacity_fixture(tool_target);
        let (stream, chunk) = append_staged_chunk(
            stream,
            vec![ContentBlock::Text {
                text: "finalized content".to_owned(),
            }],
        )
        .expect("finalized-current fixture appends its content");
        let finalize = AgentStreamCommand::Finalize {
            session_id: stream.session_id.clone(),
            stream_id: stream.stream_id.clone(),
        };
        let command =
            AgentCommand::new(revision('4'), AgentCommandAction::Stream(finalize.clone()))
                .expect("finalized-current command seals");
        AgentStreamSource::Finalize {
            session,
            stream,
            chunks: vec![chunk],
            target,
            update: None,
            resource: None,
            target_claim: None,
        }
        .reduce(&command.command_id, &finalize)
        .expect("finalized-current fixture reduces")
        .stream
    }

    #[test]
    fn finalized_stream_current_authenticates_its_exact_target_and_content() {
        for tool_target in [false, true] {
            let current = finalized_staged_stream_current(tool_target);
            current
                .verify()
                .expect("framework-derived finalized current verifies");
            let update = current
                .final_update
                .as_ref()
                .expect("finalized current retains its update");
            let content = match update {
                AgentUpdate::Message { message, .. } => &message.content,
                AgentUpdate::Tool { tool, .. } => tool
                    .output
                    .as_ref()
                    .expect("completed stream Tool retains its output"),
                _ => panic!("stream fixture emitted a non-terminal update variant"),
            };
            let expected_digest =
                canonical_digest(content).expect("final content has one canonical digest");
            validate_canonical_digest("test finalized content", &expected_digest)
                .expect("derived digest uses the raw canonical grammar");
            assert_eq!(
                current.content_digest.as_deref(),
                Some(expected_digest.as_str())
            );
        }
    }

    #[test]
    fn finalized_stream_current_rejects_target_variant_and_digest_drift() {
        let message_current = finalized_staged_stream_current(false);
        let tool_current = finalized_staged_stream_current(true);

        let mut foreign_target = message_current.clone();
        foreign_target.target = AgentStreamTarget::Message {
            message_id: "message:other-valid-target".to_owned(),
            role: MessageRole::Agent,
        };
        assert!(matches!(
            foreign_target.verify(),
            Err(ProtocolError::IdentityMismatch(message)) if message.contains("target")
        ));

        let mut foreign_role = message_current.clone();
        let AgentStreamTarget::Message { role, .. } = &mut foreign_role.target else {
            panic!("message fixture retains its target")
        };
        *role = MessageRole::System;
        assert!(matches!(
            foreign_role.verify(),
            Err(ProtocolError::IdentityMismatch(message)) if message.contains("target")
        ));

        let mut foreign_tool_target = tool_current.clone();
        foreign_tool_target.target = AgentStreamTarget::Tool {
            tool_call_id: "tool:other-valid-target".to_owned(),
        };
        assert!(matches!(
            foreign_tool_target.verify(),
            Err(ProtocolError::IdentityMismatch(message)) if message.contains("target")
        ));

        let mut different_variant = message_current.clone();
        different_variant.final_update = tool_current.final_update.clone();
        different_variant.final_update_bytes = tool_current.final_update_bytes;
        different_variant.content_digest = tool_current.content_digest.clone();
        assert!(matches!(
            different_variant.verify(),
            Err(ProtocolError::IdentityMismatch(message)) if message.contains("target")
        ));

        let mut malformed_digest = message_current.clone();
        malformed_digest.content_digest = Some(revision('e'));
        assert!(matches!(
            malformed_digest.verify(),
            Err(ProtocolError::Validation(message))
                if message.contains("64 lowercase hexadecimal")
        ));

        let mut mismatched_digest = message_current;
        let different_digest = "f".repeat(64);
        assert_ne!(
            mismatched_digest.content_digest.as_deref(),
            Some(different_digest.as_str())
        );
        mismatched_digest.content_digest = Some(different_digest);
        assert!(matches!(
            mismatched_digest.verify(),
            Err(ProtocolError::IdentityMismatch(message))
                if message.contains("content digest")
        ));
    }

    fn assert_wrapper_capacity_charged_before_append(tool_target: bool) {
        let (session, stream, target) = stream_capacity_fixture(tool_target);
        let append = AgentStreamCommand::AppendChunk {
            session_id: stream.session_id.clone(),
            stream_id: stream.stream_id.clone(),
            chunk: AgentStreamChunk {
                sequence: 0,
                content: vec![ContentBlock::Text {
                    text: "x".repeat(MAX_AGENT_VALUE_BYTES - 256),
                }],
            },
        };
        let command = AgentCommand::new(revision('3'), AgentCommandAction::Stream(append.clone()))
            .expect("chunk independently fits its canonical byte bound");
        let result = AgentStreamSource::AppendChunk {
            stream,
            current_chunk: None,
        }
        .reduce(&command.command_id, &append);
        let Ok(appended) = result else {
            assert!(matches!(result, Err(ProtocolError::Validation(_))));
            return;
        };
        let AgentStreamEffect::Chunk { current } = appended.effect else {
            panic!("Append returns its immutable chunk")
        };
        let finalize = AgentStreamCommand::Finalize {
            session_id: appended.stream.session_id.clone(),
            stream_id: appended.stream.stream_id.clone(),
        };
        let command =
            AgentCommand::new(revision('4'), AgentCommandAction::Stream(finalize.clone()))
                .expect("Finalize command seals");
        let finalized = AgentStreamSource::Finalize {
            session,
            stream: appended.stream,
            chunks: vec![current],
            target,
            update: None,
            resource: None,
            target_claim: None,
        }
        .reduce(&command.command_id, &finalize);
        assert!(
            finalized.is_ok(),
            "accepted immutable chunk cannot finalize: {finalized:?}"
        );
    }

    #[test]
    fn staged_message_wrapper_bytes_are_charged_before_append() {
        assert_wrapper_capacity_charged_before_append(false);
    }

    #[test]
    fn staged_tool_wrapper_bytes_are_charged_before_append() {
        assert_wrapper_capacity_charged_before_append(true);
    }

    fn assert_exact_final_update_byte_boundary(tool_target: bool) {
        let (session, stream, target) = stream_capacity_fixture(tool_target);
        let first = ContentBlock::Text {
            text: "escaped \" newline\n backslash\\ 🧪".to_owned(),
        };
        let probe = stream_finalization_update(
            &stream.session_id,
            &stream.stream_id,
            &stream.target,
            &target,
            &[
                first.clone(),
                ContentBlock::Text {
                    text: String::new(),
                },
            ],
        )
        .expect("capacity probe uses the real terminal update");
        let padding = MAX_AGENT_VALUE_BYTES
            - cymule_core::canonical_bytes(&probe)
                .expect("probe encodes")
                .len();
        let (stream, first_chunk) =
            append_staged_chunk(stream, vec![first]).expect("first escaped chunk appends");
        assert!(matches!(
            append_staged_chunk(
                stream.clone(),
                vec![ContentBlock::Text {
                    text: "x".repeat(padding + 1),
                }]
            ),
            Err(ProtocolError::Validation(_))
        ));
        let (stream, second_chunk) = append_staged_chunk(
            stream,
            vec![ContentBlock::Text {
                text: "x".repeat(padding),
            }],
        )
        .expect("exact final-update byte bound appends after the rejected extra byte");
        assert_eq!(stream.final_update_bytes, MAX_AGENT_VALUE_BYTES as u64);
        let finalize = AgentStreamCommand::Finalize {
            session_id: stream.session_id.clone(),
            stream_id: stream.stream_id.clone(),
        };
        let command =
            AgentCommand::new(revision('4'), AgentCommandAction::Stream(finalize.clone()))
                .expect("exact-capacity Finalize command seals");
        let source = AgentStreamSource::Finalize {
            session,
            stream,
            chunks: vec![first_chunk, second_chunk],
            target,
            update: None,
            resource: None,
            target_claim: None,
        };
        let finalized = source
            .reduce(&command.command_id, &finalize)
            .expect("exact-capacity staged stream finalizes");
        assert_eq!(
            cymule_core::canonical_bytes(
                finalized
                    .stream
                    .final_update
                    .as_ref()
                    .expect("final update retained"),
            )
            .expect("final update encodes")
            .len(),
            MAX_AGENT_VALUE_BYTES
        );
        verify_capacity_receipt_and_tampering(&command, source, finalized);
    }

    fn verify_capacity_receipt_and_tampering(
        command: &AgentCommand,
        source: AgentStreamSource,
        finalized: AgentStreamPostcondition,
    ) {
        let receipt = AgentCommandReceipt::new(
            command,
            AgentCommandSource::Stream(Box::new(source.clone())),
            AgentCommandOutcome::Stream(finalized.clone()),
        )
        .expect("exact-capacity receipt verifies");
        let reopened: AgentCommandReceipt = cymule_core::decode_json(
            &cymule_core::canonical_bytes(&receipt).expect("receipt encodes"),
        )
        .expect("exact-capacity receipt reopens");
        reopened
            .verify_for(command)
            .expect("reopened capacity receipt verifies");

        let mut wrong_current = finalized.stream.clone();
        wrong_current.final_update_bytes -= 1;
        assert!(wrong_current.verify().is_err());
        let mut wrong_source = source;
        let AgentStreamSource::Finalize { stream, .. } = &mut wrong_source else {
            panic!("fixture retains its exact Finalize source")
        };
        stream.final_update_bytes -= 1;
        assert!(
            AgentCommandReceipt::new(
                command,
                AgentCommandSource::Stream(Box::new(wrong_source)),
                AgentCommandOutcome::Stream(finalized),
            )
            .is_err()
        );
    }

    #[test]
    fn staged_message_wrapper_has_an_exact_cross_chunk_byte_boundary() {
        assert_exact_final_update_byte_boundary(false);
    }

    #[test]
    fn staged_tool_wrapper_has_an_exact_cross_chunk_byte_boundary() {
        assert_exact_final_update_byte_boundary(true);
    }

    #[test]
    fn staged_tool_wrapper_cannot_change_while_its_target_is_in_progress() {
        let (session, _, target) = stream_capacity_fixture(true);
        let AgentStreamTargetSource::Tool {
            current: Some(current),
        } = target
        else {
            panic!("fixture owns its in-progress Tool current")
        };
        let mut tool = current.tool.clone();
        tool.locations
            .push("source:changed-presentation".to_owned());
        let update = AgentUpdate::Tool {
            update_id: "update:changed-wrapper".to_owned(),
            tool,
        };
        assert!(matches!(
            session.reduce_update(
                &revision('5'),
                &update,
                &AgentSessionUpdateSource {
                    update: None,
                    target_claims: absent_tool_claim(&current.tool.tool_call_id),
                    entry: AgentSessionEntrySource::Tool {
                        current: Some(current)
                    },
                }
            ),
            Err(ProtocolError::IllegalTransition(_))
        ));
    }

    #[test]
    fn staged_stream_rejects_cumulative_content_overflow_before_append() {
        let open = AgentStreamCommand::Open {
            session_id: "session:content-bound".to_owned(),
            stream_id: "stream:content-bound".to_owned(),
            target: AgentStreamTarget::Message {
                message_id: "message:content-bound".to_owned(),
                role: MessageRole::Agent,
            },
            delivery: AgentStreamDelivery::Staged,
        };
        let (_, stream) = open_message_stream(&open, '1');
        let (stream, _) = append_staged_content(stream, 200);
        for second_count in [57, 200] {
            let second = AgentStreamCommand::AppendChunk {
                session_id: stream.session_id.clone(),
                stream_id: stream.stream_id.clone(),
                chunk: AgentStreamChunk {
                    sequence: stream.next_chunk_sequence,
                    content: vec![
                        ContentBlock::Text {
                            text: "x".to_owned()
                        };
                        second_count
                    ],
                },
            };
            let command =
                AgentCommand::new(revision('3'), AgentCommandAction::Stream(second.clone()))
                    .expect("second chunk is individually within the content bound");
            assert!(
                AgentStreamSource::AppendChunk {
                    stream: stream.clone(),
                    current_chunk: None,
                }
                .reduce(&command.command_id, &second)
                .is_err(),
                "a second chunk must not admit {} terminal content blocks",
                200 + second_count
            );
        }
        stream
            .verify()
            .expect("rejected append preserves its source");
    }

    #[test]
    fn staged_stream_finalizes_exact_content_capacity_and_verifies_counter() {
        let open = AgentStreamCommand::Open {
            session_id: "session:exact-content-bound".to_owned(),
            stream_id: "stream:exact-content-bound".to_owned(),
            target: AgentStreamTarget::Message {
                message_id: "message:exact-content-bound".to_owned(),
                role: MessageRole::Agent,
            },
            delivery: AgentStreamDelivery::Staged,
        };
        let (session, mut stream) = open_message_stream(&open, '1');
        let mut chunks = Vec::new();
        for count in [200, 56] {
            let (next, current) = append_staged_content(stream, count);
            chunks.push(current);
            stream = next;
        }
        assert_eq!(stream.staged_content_blocks, MAX_AGENT_VALUE_ENTRIES as u64);
        let finalize = AgentStreamCommand::Finalize {
            session_id: stream.session_id.clone(),
            stream_id: stream.stream_id.clone(),
        };
        let command =
            AgentCommand::new(revision('3'), AgentCommandAction::Stream(finalize.clone()))
                .expect("finalize command seals");
        let source = AgentStreamSource::Finalize {
            session,
            stream,
            chunks,
            target: AgentStreamTargetSource::Message { current: None },
            update: None,
            resource: None,
            target_claim: None,
        };
        let postcondition = source
            .reduce(&command.command_id, &finalize)
            .expect("exactly 256 content blocks finalize");
        let Some(AgentUpdate::Message { message, .. }) = &postcondition.stream.final_update else {
            panic!("staged stream finalizes one message")
        };
        assert_eq!(message.content.len(), MAX_AGENT_VALUE_ENTRIES);
        let receipt = AgentCommandReceipt::new(
            &command,
            AgentCommandSource::Stream(Box::new(source.clone())),
            AgentCommandOutcome::Stream(postcondition.clone()),
        )
        .expect("exact-bound finalize receipt verifies");
        let reopened: AgentCommandReceipt = cymule_core::decode_json(
            &cymule_core::canonical_bytes(&receipt).expect("finalize receipt encodes"),
        )
        .expect("finalize receipt reopens");
        reopened
            .verify_for(&command)
            .expect("reopened finalize receipt verifies");

        let mut wrong_count = source;
        let AgentStreamSource::Finalize { stream, .. } = &mut wrong_count else {
            panic!("fixture retains finalization source")
        };
        stream.staged_content_blocks -= 1;
        assert!(wrong_count.reduce(&command.command_id, &finalize).is_err());
        assert!(
            AgentCommandReceipt::new(
                &command,
                AgentCommandSource::Stream(Box::new(wrong_count)),
                AgentCommandOutcome::Stream(postcondition),
            )
            .is_err()
        );
    }

    fn assert_foreign_target_reservation_is_rejected(
        source: &AgentStreamSource,
        command: &AgentCommand,
        product: &AgentStreamPublicationProduct,
    ) {
        let mut wrong = source.clone();
        let AgentStreamSource::Finalize {
            stream,
            target_claim: Some(target_claim),
            ..
        } = &mut wrong
        else {
            panic!("reserved fixture retains its stream and target claim")
        };
        let reservation = stream
            .publication_reservation
            .as_ref()
            .expect("reserved fixture retains its publication reservation");
        **target_claim = AgentTargetClaimCurrent::new(
            &stream.session_id,
            AgentTargetClaimTarget::from_stream_target(&stream.target),
            target_claim.generation,
            target_claim.predecessor_claim_id.as_deref(),
            target_claim.predecessor_admitted_by.as_deref(),
            AgentTargetClaimPhase::Reserved {
                stream_id: stream.stream_id.clone(),
                reservation_id: revision('e'),
            },
            reservation.intent.command_id(),
        )
        .expect("foreign reservation target claim seals independently");
        assert!(
            wrong.reduce_with_publication(command, product).is_err(),
            "publication cannot consume a target claim owned by another reservation"
        );
    }

    #[test]
    fn external_stream_provider_is_preflighted_and_couples_resource_pin() {
        let (finalize_command, finalize, source) = external_stream_finalize_fixture();
        assert!(
            source
                .reduce(&finalize_command.command_id, &finalize)
                .is_err()
        );

        let mut providers = TestAgentProviders {
            publication: Some(external_publication()),
            ..TestAgentProviders::default()
        };
        let (source, reservation) = reserve_external_stream_source(&finalize_command, source);
        let result = execute_agent_stream_publication(&reservation, &mut providers)
            .expect("registered external publication resolves");
        let AgentStreamPublicationResult::Published { product, .. } = result else {
            panic!("test provider must return a read-back publication")
        };
        assert_eq!(providers.publication_calls, 1);
        assert_eq!(
            providers.publication_intents,
            vec![product.intent().clone()]
        );
        assert_eq!(product.intent().session_id(), "session:external");
        assert_eq!(product.intent().stream_id(), "stream:external");
        assert_eq!(product.intent().command_id(), finalize_command.command_id);
        assert_eq!(product.intent().resolver_binding(), "resolver:test/1");
        assert_eq!(product.intent().content().digest, revision('a'));
        assert_eq!(
            product
                .intent()
                .resource_handle()
                .expect("intent derives its unique Resource Handle"),
            product.publication().resource,
        );
        assert_eq!(
            product.publication().locators.resolver_binding,
            "resolver:test/1"
        );
        assert!(matches!(
            product.resource_profile_pin().pin.kind,
            ResourcePinKind::AgentStream {
                ref session_id,
                ref stream_id,
            } if session_id == "session:external" && stream_id == "stream:external"
        ));
        let mut wrong_reservation_origin = source.clone();
        let AgentStreamSource::Finalize {
            resource: Some(resource),
            ..
        } = &mut wrong_reservation_origin
        else {
            panic!("reserved fixture retains its Resource source")
        };
        resource
            .pin
            .as_mut()
            .expect("reserved fixture retains its Resource pin")
            .last_receipt = ResourceLifecycleReceiptRef::from_agent_publication_reservation(
            finalize_command.command_id.clone(),
            "session:external",
            "stream:external",
            revision('f'),
        )
        .expect("foreign reservation origin seals");
        assert!(
            wrong_reservation_origin
                .reduce_with_publication(&finalize_command, &product)
                .is_err(),
            "publication cannot promote a pin owned by another reservation"
        );
        assert_foreign_target_reservation_is_rejected(&source, &finalize_command, &product);
        let finalized = source
            .reduce_with_publication(&finalize_command, &product)
            .expect("authorized external publication finalizes");
        assert_eq!(finalized.stream.staged_content_blocks, 0);
        assert!(matches!(
            &finalized.stream.final_update,
            Some(AgentUpdate::Message { message, .. })
                if matches!(message.content.as_slice(), [ContentBlock::ResourceHandle { .. }])
        ));
        let receipt = AgentCommandReceipt::new(
            &finalize_command,
            AgentCommandSource::Stream(Box::new(source)),
            AgentCommandOutcome::Stream(finalized),
        )
        .expect("external finalization receipt replays exact publication and pin");
        assert!(
            receipt
                .resource_pin_receipt_for(&finalize_command)
                .expect("Resource pin receipt resolves")
                .is_some()
        );
    }

    #[test]
    fn external_stream_abort_releases_only_a_durably_not_applied_reservation() {
        let (finalize_command, _, source) = external_stream_finalize_fixture();
        let (source, _) = reserve_external_stream_source(&finalize_command, source);
        let AgentStreamSource::Finalize {
            session,
            stream,
            resource: Some(resource),
            target_claim: Some(target_claim),
            ..
        } = source
        else {
            panic!("reserved external fixture retains its exact Resource source")
        };
        let abort = AgentStreamCommand::Abort {
            session_id: stream.session_id.clone(),
            stream_id: stream.stream_id.clone(),
            reason: "caller:no-output".to_owned(),
        };
        let abort_command =
            AgentCommand::new(revision('5'), AgentCommandAction::Stream(abort.clone()))
                .expect("Abort command seals");
        let claimed = AgentStreamSource::Abort {
            session: session.clone(),
            stream: stream.clone(),
            resource: Some(resource.clone()),
            target_claim: Some(target_claim.clone()),
        };
        assert!(matches!(
            claimed.reduce(&abort_command.command_id, &abort),
            Err(ProtocolError::Conflict { ref code, .. })
                if code == "agent_stream_publication_abort_unresolved"
        ));

        let stream = mark_agent_stream_publication_not_applied(&stream, &finalize_command)
            .expect("provider-proved NotApplied becomes durable");
        let source = AgentStreamSource::Abort {
            session,
            stream,
            resource: Some(resource),
            target_claim: Some(target_claim),
        };
        let postcondition = source
            .reduce(&abort_command.command_id, &abort)
            .expect("NotApplied reservation aborts atomically");
        assert_eq!(postcondition.stream.state, AgentStreamState::Aborted);
        assert!(postcondition.stream.publication_reservation.is_none());
        let AgentStreamEffect::Aborted {
            session,
            resource_release_receipt: Some(release),
        } = &postcondition.effect
        else {
            panic!("NotApplied abort retains Session and Resource release")
        };
        assert_eq!(session.open_stream_count, 0);
        assert_eq!(release.active_pin_count, 0);
        let expected_release = release.clone();
        let receipt = AgentCommandReceipt::new(
            &abort_command,
            AgentCommandSource::Stream(Box::new(source)),
            AgentCommandOutcome::Stream(postcondition),
        )
        .expect("Abort receipt replays its exact reserved release");
        let wire = serde_json::to_value(&receipt).expect("Abort receipt encodes");
        let reopened: AgentCommandReceipt = serde_json::from_value(wire.clone())
            .expect("Abort receipt required-nullable fields reopen");
        reopened
            .verify_for(&abort_command)
            .expect("reopened Abort receipt verifies");
        for pointer in [
            "/source/source/resource",
            "/outcome/receipt/effect/resource_release_receipt",
        ] {
            let mut missing = wire.clone();
            let (parent, field) = pointer.rsplit_once('/').expect("fixture pointer splits");
            missing
                .pointer_mut(parent)
                .and_then(Value::as_object_mut)
                .expect("fixture parent is an object")
                .remove(field);
            assert!(serde_json::from_value::<AgentCommandReceipt>(missing).is_err());
        }
        assert_eq!(
            receipt
                .resource_release_receipt_for(&abort_command)
                .expect("Abort Resource release resolves"),
            Some(&expected_release)
        );
    }

    #[test]
    fn external_stream_current_rejects_a_self_consistent_foreign_target_reservation() {
        let (command_a, _, source_a) = external_stream_finalize_fixture_for(
            "session:target-edge",
            "stream:target-edge",
            "message:target-a",
        );
        let (source_a, _) = reserve_external_stream_source(&command_a, source_a);
        let (command_b, _, source_b) = external_stream_finalize_fixture_for(
            "session:target-edge",
            "stream:target-edge",
            "message:target-b",
        );
        assert_eq!(command_a, command_b);
        let (_, reservation_b) = reserve_external_stream_source(&command_b, source_b);
        reservation_b
            .verify()
            .expect("target B reservation is independently self-consistent");
        assert!(matches!(
            reservation_b.intent.target(),
            AgentStreamTarget::Message { message_id, .. } if message_id == "message:target-b"
        ));

        let AgentStreamSource::Finalize { mut stream, .. } = source_a else {
            panic!("target A fixture is an external Finalize source")
        };
        assert!(matches!(
            &stream.target,
            AgentStreamTarget::Message { message_id, .. } if message_id == "message:target-a"
        ));
        stream.publication_reservation = Some(Box::new(reservation_b));
        assert!(matches!(
            stream.verify(),
            Err(ProtocolError::IdentityMismatch(message)) if message.contains("target")
        ));
    }

    #[test]
    fn external_stream_unknown_retains_exact_intent_for_provider_reconciliation() {
        let (command, _, source) = external_stream_finalize_fixture();
        let (_, reservation) = reserve_external_stream_source(&command, source);
        let mut providers = TestAgentProviders {
            publication_observation: Some(AgentStreamPublicationObservation::Unknown),
            reconciliation_observation: Some(AgentStreamPublicationObservation::Published {
                publication: Box::new(external_publication()),
            }),
            ..TestAgentProviders::default()
        };
        let first = execute_agent_stream_publication(&reservation, &mut providers)
            .expect("ambiguous publication returns a typed result");
        let AgentStreamPublicationResult::Unknown { intent, .. } = first else {
            panic!("ambiguous provider result must retain its exact intent")
        };
        let intent_bytes = cymule_core::canonical_bytes(&intent).expect("Unknown intent encodes");
        let restored: AgentStreamPublicationIntent =
            cymule_core::decode_json(&intent_bytes).expect("Unknown intent restores after restart");
        restored.verify().expect("restored intent verifies");
        assert_eq!(restored, intent);

        let reconciled =
            reconcile_agent_stream_publication(&reservation, &restored, &mut providers)
                .expect("provider-ledger publication reconciliation resolves");
        let AgentStreamPublicationResult::Published { product, .. } = reconciled else {
            panic!("exact readback must return the verified publication product")
        };
        assert_eq!(product.intent(), &intent);
        assert_eq!(providers.publication_calls, 1);
        assert_eq!(providers.publication_observation_calls, 1);
        assert_eq!(
            providers.publication_intents,
            vec![intent.clone(), intent.clone()]
        );
        let not_applied = reservation
            .mark_not_applied()
            .expect("NotApplied reservation derives");
        assert!(matches!(
            reconcile_agent_stream_publication(&not_applied, &restored, &mut providers),
            Err(ProtocolError::Conflict { ref code, .. })
                if code == "agent_stream_publication_dispatch_not_claimed"
        ));
        assert_eq!(providers.publication_observation_calls, 1);

        let (_, _, drifted_source) = external_stream_finalize_fixture_for(
            "session:external-drifted",
            "stream:external-drifted",
            "message:external-drifted",
        );
        let (drifted_source, drifted) = reserve_external_stream_source(
            &AgentCommand::new(
                revision('4'),
                AgentCommandAction::Stream(AgentStreamCommand::Finalize {
                    session_id: "session:external-drifted".to_owned(),
                    stream_id: "stream:external-drifted".to_owned(),
                }),
            )
            .expect("drifted command seals"),
            drifted_source,
        );
        drop(drifted_source);
        assert!(matches!(
            reconcile_agent_stream_publication(&drifted, &intent, &mut providers),
            Err(ProtocolError::Conflict { ref code, .. })
                if code == "agent_stream_publication_intent_changed"
        ));
        assert_eq!(providers.publication_observation_calls, 1);
    }

    #[test]
    fn external_stream_provider_rejects_chunks_and_binding_substitution() {
        let (_, _, source) = external_stream_finalize_fixture();
        let AgentStreamSource::Finalize { stream, .. } = &source else {
            panic!("fixture is an external finalization")
        };
        let append = AgentStreamCommand::AppendChunk {
            session_id: "session:external".to_owned(),
            stream_id: "stream:external".to_owned(),
            chunk: AgentStreamChunk {
                sequence: 0,
                content: vec![ContentBlock::Text {
                    text: "chunk".to_owned(),
                }],
            },
        };
        let append_command =
            AgentCommand::new(revision('2'), AgentCommandAction::Stream(append.clone()))
                .expect("append command seals");
        assert!(
            AgentStreamSource::AppendChunk {
                stream: stream.clone(),
                current_chunk: None,
            }
            .reduce(&append_command.command_id, &append)
            .is_err()
        );

        let mut forged = external_publication();
        forged.locators.resolver_binding = "fixture:different-store".to_owned();
        let (finalize_command, _, source) = external_stream_finalize_fixture();
        let mut providers = TestAgentProviders {
            publication: Some(forged),
            ..TestAgentProviders::default()
        };
        let (_, reservation) = reserve_external_stream_source(&finalize_command, source);
        assert!(execute_agent_stream_publication(&reservation, &mut providers).is_err());
        assert_eq!(providers.publication_calls, 1);

        let (finalize_command, _, source) = external_stream_finalize_fixture();
        let mut providers = TestAgentProviders {
            publication: Some(external_publication_with_digest('b')),
            ..TestAgentProviders::default()
        };
        let (_, reservation) = reserve_external_stream_source(&finalize_command, source);
        assert!(execute_agent_stream_publication(&reservation, &mut providers).is_err());
        assert_eq!(providers.publication_calls, 1);
    }

    #[test]
    fn provider_not_applied_fence_prevents_a_late_stale_dispatch_write() {
        let (command, _, source) = external_stream_finalize_fixture();
        let (_, reservation) = reserve_external_stream_source(&command, source);
        let intent = reservation.intent.clone();
        let mut providers = TestAgentProviders {
            publication: Some(external_publication()),
            reconciliation_observation: Some(AgentStreamPublicationObservation::NotApplied),
            ..TestAgentProviders::default()
        };

        let reconciled = reconcile_agent_stream_publication(&reservation, &intent, &mut providers)
            .expect("provider ledger fences the exact dispatch as NotApplied");
        assert!(matches!(
            reconciled,
            AgentStreamPublicationResult::NotApplied { .. }
        ));

        let stale = execute_agent_stream_publication(&reservation, &mut providers)
            .expect("stale publisher observes the provider-side tombstone");
        assert!(matches!(
            stale,
            AgentStreamPublicationResult::NotApplied { .. }
        ));
        assert_eq!(providers.publication_observation_calls, 1);
        assert_eq!(providers.publication_calls, 1);
        assert!(matches!(
            providers
                .publication_dispatches
                .get(&reservation.dispatch_id),
            Some(AgentStreamPublicationObservation::NotApplied)
        ));
    }

    #[test]
    fn external_stream_publication_product_cannot_cross_streams() {
        let (first_command, _, first_source) = external_stream_finalize_fixture();
        let mut providers = TestAgentProviders {
            publication: Some(external_publication()),
            ..TestAgentProviders::default()
        };
        let (_, first_reservation) = reserve_external_stream_source(&first_command, first_source);
        let result = execute_agent_stream_publication(&first_reservation, &mut providers)
            .expect("first stream resolves its pinned publication");
        let AgentStreamPublicationResult::Published { product, .. } = result else {
            panic!("test provider must return a read-back publication")
        };

        let (second_command, _, second_source) = external_stream_finalize_fixture_for(
            "session:external-other",
            "stream:external-other",
            "message:external-other",
        );
        let (second_source, _) = reserve_external_stream_source(&second_command, second_source);
        let error = second_source
            .reduce_with_publication(&second_command, &product)
            .expect_err("a provider product cannot cross its preflight stream");
        assert!(matches!(error, ProtocolError::IdentityMismatch(_)));
    }

    #[test]
    fn external_stream_provider_preflight_rejects_before_io() {
        let (command, _, mut source) = external_stream_finalize_fixture();
        let AgentStreamSource::Finalize { stream, .. } = &mut source else {
            panic!("fixture is an external finalization")
        };
        stream.state = AgentStreamState::Finalized;
        let providers = TestAgentProviders {
            publication: Some(external_publication()),
            ..TestAgentProviders::default()
        };
        assert!(reserve_agent_stream_publication(&source, &command).is_err());
        assert_eq!(providers.publication_calls, 0);

        let (command, _, mut source) = external_stream_finalize_fixture();
        let AgentStreamSource::Finalize { resource, .. } = &mut source else {
            panic!("fixture is an external finalization")
        };
        *resource = Some(Box::new(AgentStreamResourceSource {
            retention: None,
            pin: None,
        }));
        let mut providers = TestAgentProviders {
            publication: Some(external_publication()),
            ..TestAgentProviders::default()
        };
        let reserved = reserve_agent_stream_publication(&source, &command)
            .expect("exact Resource source admits reservation before I/O");
        let reservation = reserved
            .stream
            .publication_reservation
            .as_ref()
            .expect("reservation persists");
        assert!(execute_agent_stream_publication(reservation, &mut providers).is_ok());
        assert_eq!(providers.publication_calls, 1);
    }

    #[test]
    fn external_stream_provider_product_is_not_wire_input() {
        let (finalize_command, _, source) = external_stream_finalize_fixture();
        let mut serialized = serde_json::to_value(&source).expect("source encodes");
        serialized
            .as_object_mut()
            .expect("source is tagged object")
            .insert(
                "publication".to_owned(),
                serde_json::to_value(external_publication()).expect("publication encodes"),
            );
        assert!(serde_json::from_value::<AgentStreamSource>(serialized).is_err());

        let mut serialized_command =
            serde_json::to_value(&finalize_command).expect("finalize command encodes");
        serialized_command["action"]["command"]
            .as_object_mut()
            .expect("stream command is a tagged object")
            .insert(
                "publication".to_owned(),
                serde_json::to_value(external_publication()).expect("publication encodes"),
            );
        assert!(serde_json::from_value::<AgentCommand>(serialized_command).is_err());
    }

    #[test]
    fn workspace_start_uses_registered_binding_and_exact_m1_receipt() {
        let fixture = workspace_test_fixture();
        let started = start_workspace(&fixture);
        assert_eq!(
            started.occurrence.current.occurrence.state,
            AgentHostOccurrenceState::Started
        );
        AgentCommandReceipt::new(
            &fixture.command,
            AgentCommandSource::Workspace(fixture.source.clone()),
            AgentCommandOutcome::Workspace(Box::new(started.clone())),
        )
        .expect("workspace start receipt replays exact postcondition");

        let mut providers = TestAgentProviders {
            workspace_binding: Some(
                AgentHostBinding::standalone("workspace-host:test/1", "binding:wrong/1")
                    .expect("standalone binding constructs"),
            ),
            ..TestAgentProviders::default()
        };
        let product =
            execute_agent_workspace_provider(&fixture.source, &fixture.command, &mut providers)
                .expect("registered but mismatched binding resolves as a product");
        assert_eq!(providers.binding_calls, 1);
        assert!(
            fixture
                .source
                .reduce_with_provider(
                    &fixture.command.command_id,
                    &fixture.start,
                    &product,
                    AgentWorkspaceM1Witness {
                        run_id: fixture.request.run_id,
                        scope_id: fixture.request.scope_id,
                        phase: AgentWorkspaceCommandPhase::StartEffectDispatch,
                        continuation_digest: revision('7'),
                        effect_intent_id: Some(fixture.effect_intent_id),
                        obligation_id: Some(fixture.obligation_id),
                        m1_receipt_id: revision('8'),
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn workspace_effect_lease_is_required_only_for_start_and_binds_clock_scope() {
        let fixture = workspace_test_fixture();
        fixture.start.verify().expect("Effect start lease verifies");

        let mut missing = fixture.request.clone();
        missing.dispatch_lease = None;
        let AgentWorkspaceCommand::StartEffect {
            effect_intent_id,
            execution_binding,
            operation_occurrence_binding,
            ..
        } = &fixture.start
        else {
            panic!("fixture is an Effect start")
        };
        assert!(
            AgentWorkspaceCommand::StartEffect {
                request: missing.clone(),
                effect_intent_id: effect_intent_id.clone(),
                execution_binding: execution_binding.clone(),
                operation_occurrence_binding: operation_occurrence_binding.clone(),
            }
            .verify()
            .is_err()
        );
        assert!(
            AgentWorkspaceCommand::SettleEffect {
                request: fixture.request.clone(),
            }
            .verify()
            .is_err()
        );

        let mut wrong_owner = fixture.request;
        wrong_owner
            .dispatch_lease
            .as_mut()
            .expect("fixture has a lease")
            .owner = "claim:other".to_owned();
        assert!(wrong_owner.verify().is_err());
        missing
            .verify()
            .expect("settlement owner without a lease verifies");
    }

    fn workspace_evidence(bytes: Vec<u8>) -> cymule_core::ArtifactRecord {
        cymule_core::ArtifactRecord {
            reference: cymule_core::artifact_ref("test.workspace-evidence/1", &bytes).unwrap(),
            bytes,
        }
    }

    #[test]
    fn workspace_observation_material_owns_only_exact_typed_references() {
        let record = workspace_evidence(b"fresh-evidence".to_vec());
        let unrelated = workspace_evidence(b"json-is-not-an-artifact-reference".to_vec());
        let observation = AgentWorkspaceObservation {
            resolution: AgentOccurrenceResolution::Unknown {
                evidence: vec![
                    ContentBlock::Artifact {
                        artifact: record.reference.clone(),
                    },
                    ContentBlock::Json {
                        value: serde_json::to_value(&unrelated.reference).unwrap(),
                    },
                ],
            },
            artifacts: vec![record.clone()],
        };
        observation
            .verify()
            .expect("new typed evidence carries exact bytes");
        let product = AgentWorkspaceProviderProduct::observed(observation.clone()).unwrap();
        assert_eq!(
            product.required_artifacts().unwrap(),
            BTreeSet::from([record.reference.clone()])
        );
        assert_eq!(product.artifacts(), &[record]);
        let mut parent_reuse = observation.clone();
        parent_reuse.artifacts.clear();
        parent_reuse
            .verify()
            .expect("Durable must resolve omitted records from the exact parent");
        let mut extra = observation.clone();
        extra.artifacts.push(unrelated);
        assert!(extra.verify().is_err());
        let mut duplicate = observation.clone();
        duplicate.artifacts.push(duplicate.artifacts[0].clone());
        assert!(duplicate.verify().is_err());
        let mut changed_bytes = observation;
        changed_bytes.artifacts[0].bytes.push(0);
        assert!(changed_bytes.verify().is_err());
    }

    #[test]
    fn workspace_observation_material_enforces_one_aggregate_byte_budget() {
        let first = workspace_evidence(vec![0; MAX_AGENT_WORKSPACE_ARTIFACT_BYTES / 2]);
        let second = workspace_evidence(vec![1; MAX_AGENT_WORKSPACE_ARTIFACT_BYTES / 2]);
        let mut observation = AgentWorkspaceObservation {
            resolution: AgentOccurrenceResolution::Unknown {
                evidence: vec![
                    ContentBlock::Artifact {
                        artifact: first.reference.clone(),
                    },
                    ContentBlock::Artifact {
                        artifact: second.reference.clone(),
                    },
                ],
            },
            artifacts: vec![first, second],
        };
        observation
            .verify()
            .expect("the exact aggregate byte ceiling is valid");
        let record = &mut observation.artifacts[1];
        record.bytes.push(1);
        record.reference =
            cymule_core::artifact_ref(&record.reference.kind, &record.bytes).unwrap();
        let AgentOccurrenceResolution::Unknown { evidence } = &mut observation.resolution else {
            unreachable!();
        };
        evidence[1] = ContentBlock::Artifact {
            artifact: record.reference.clone(),
        };
        assert!(matches!(
            observation.verify(),
            Err(ProtocolError::Validation(_))
        ));
    }

    #[test]
    fn workspace_preview_prepares_started_authority_without_dispatch() {
        let fixture = workspace_test_fixture();
        let mut providers = TestAgentProviders {
            workspace_binding: Some(fixture.binding.clone()),
            ..TestAgentProviders::default()
        };
        let product =
            execute_agent_workspace_provider(&fixture.source, &fixture.command, &mut providers)
                .expect("workspace provider resolves the exact binding");
        let occurrence = fixture
            .source
            .preview_occurrence(&fixture.start, &product)
            .expect("workspace preview derives the exact Started occurrence");
        assert_eq!(occurrence.state, AgentHostOccurrenceState::Started);
        assert_eq!(occurrence.occurrence_binding, fixture.binding);
        assert_eq!(providers.binding_calls, 1);
        assert_eq!(providers.workspace_dispatch_calls, 0);
        assert_eq!(providers.observation_calls, 0);
        assert_eq!(
            fixture
                .source
                .preview_occurrence(&fixture.start, &product)
                .unwrap(),
            occurrence,
        );
        assert_eq!(providers.workspace_dispatch_calls, 0);
    }

    #[test]
    fn workspace_body_capacity_preserves_the_largest_legal_terminal_receipt() {
        let fixture = workspace_test_fixture();
        let started = start_workspace(&fixture).occurrence.current.occurrence;
        let suffix = "/1";
        let kind = format!(
            "{}{suffix}",
            "z".repeat(cymule_core::MAX_ARTIFACT_KIND_BYTES - suffix.len()),
        );
        let response = AgentHostResponse::Workspace(WorkspaceReceipt {
            change_id: fixture.request.change_id,
            committed: true,
            evidence: cymule_core::artifact_ref(kind, b"actual terminal evidence").unwrap(),
            occurrence_binding: fixture.binding.binding_id().to_owned(),
        });
        let seed = started
            .mark_unknown_with_evidence(
                "provider observation",
                vec![ContentBlock::Text {
                    text: String::new(),
                }],
            )
            .unwrap();
        let terminal = seed.complete(response.clone()).unwrap();
        let padding =
            MAX_AGENT_VALUE_BYTES - cymule_core::canonical_bytes(&terminal).unwrap().len();
        let boundary = started
            .mark_unknown_with_evidence(
                "provider observation",
                vec![ContentBlock::Text {
                    text: "x".repeat(padding),
                }],
            )
            .unwrap();
        verify_workspace_terminal_body_capacity(&boundary).unwrap();
        let completed = boundary.complete(response.clone()).unwrap();
        assert_eq!(
            cymule_core::canonical_bytes(&completed).unwrap().len(),
            MAX_AGENT_VALUE_BYTES
        );
        let overflow = started
            .mark_unknown_with_evidence(
                "provider observation",
                vec![ContentBlock::Text {
                    text: "x".repeat(padding + 1),
                }],
            )
            .unwrap();
        overflow.validate().unwrap();
        assert!(verify_workspace_terminal_body_capacity(&overflow).is_err());
        assert!(overflow.complete(response).is_err());
    }

    #[test]
    fn duplicate_workspace_unknown_has_typed_unchanged_outcome_without_receipt() {
        let fixture = workspace_test_fixture();
        let started = start_workspace(&fixture);
        let mut request = fixture.request;
        request.dispatch_lease = None;
        let settle = AgentWorkspaceCommand::SettleEffect { request };
        let command = AgentCommand::new(
            revision('4'),
            AgentCommandAction::Workspace(Box::new(settle.clone())),
        )
        .expect("workspace settlement command seals");
        let mut current = started.occurrence.current;
        current.occurrence = current
            .occurrence
            .mark_unknown("provider response was lost")
            .expect("workspace occurrence becomes Unknown")
            .mark_unknown_with_evidence(
                "ignored replacement",
                vec![ContentBlock::Text {
                    text: "provider readback remains inconclusive".to_owned(),
                }],
            )
            .expect("workspace observation appends");
        let outcome = AgentWorkspaceCommitOutcome::Unchanged {
            command_id: command.command_id.clone(),
            observed_revision: revision('5'),
            current: Box::new(current),
        };
        outcome
            .verify_for(&command)
            .expect("duplicate evidence can return the exact unchanged current");

        let mut wrong = outcome;
        let AgentWorkspaceCommitOutcome::Unchanged { command_id, .. } = &mut wrong else {
            unreachable!()
        };
        *command_id = revision('6');
        assert!(wrong.verify_for(&command).is_err());
    }

    #[test]
    fn workspace_settlement_uses_provider_observation_and_exact_m1_receipt() {
        let fixture = workspace_test_fixture();
        let started = start_workspace(&fixture);
        let mut settle_request = fixture.request.clone();
        settle_request.dispatch_lease = None;
        let settle = AgentWorkspaceCommand::SettleEffect {
            request: settle_request,
        };
        let settle_command = AgentCommand::new(
            revision('4'),
            AgentCommandAction::Workspace(Box::new(settle.clone())),
        )
        .expect("workspace settlement command seals");
        let settle_source = AgentWorkspaceSource {
            occurrence: AgentOccurrenceSource {
                session: started.occurrence.session,
                current: Some(started.occurrence.current),
            },
        };
        let response = AgentHostResponse::Workspace(WorkspaceReceipt {
            change_id: fixture.request.change_id.clone(),
            committed: true,
            evidence: cymule_core::artifact_ref("workspace/evidence", b"committed")
                .expect("provider evidence Artifact derives"),
            occurrence_binding: fixture.binding.binding_id().to_owned(),
        });
        let mut providers = TestAgentProviders {
            workspace_resolution: Some(AgentOccurrenceResolution::Completed { response }),
            ..TestAgentProviders::default()
        };
        let product =
            execute_agent_workspace_provider(&settle_source, &settle_command, &mut providers)
                .expect("binding-pinned provider observation resolves");
        assert_eq!(providers.observation_calls, 1);
        let settled = settle_source
            .reduce_with_provider(
                &settle_command.command_id,
                &settle,
                &product,
                AgentWorkspaceM1Witness {
                    run_id: fixture.request.run_id,
                    scope_id: fixture.request.scope_id,
                    phase: AgentWorkspaceCommandPhase::SettleEffectApplied,
                    continuation_digest: revision('5'),
                    effect_intent_id: Some(fixture.effect_intent_id),
                    obligation_id: Some(fixture.obligation_id),
                    m1_receipt_id: revision('6'),
                },
            )
            .expect("workspace settlement couples provider result and M1 closure");
        assert_eq!(
            settled.occurrence.current.occurrence.state,
            AgentHostOccurrenceState::Completed
        );
        let receipt = AgentCommandReceipt::new(
            &settle_command,
            AgentCommandSource::Workspace(settle_source.clone()),
            AgentCommandOutcome::Workspace(Box::new(settled.clone())),
        )
        .expect("workspace settlement receipt replays exactly");
        receipt
            .verify_for(&settle_command)
            .expect("workspace settlement profile projection replays exactly");

        let mut wrong_run = settled;
        wrong_run.m1.run_id = "run:different".to_owned();
        assert!(wrong_run.verify_for(&settle).is_err());
        let mut missing_m1 = serde_json::to_value(receipt).expect("receipt encodes");
        missing_m1["outcome"]["receipt"]
            .as_object_mut()
            .expect("workspace checkpoint is an object")
            .remove("m1");
        assert!(serde_json::from_value::<AgentCommandReceipt>(missing_m1).is_err());
    }

    #[test]
    fn workspace_provider_product_is_not_wire_input() {
        let fixture = workspace_test_fixture();
        let mut serialized_source =
            serde_json::to_value(&fixture.source).expect("workspace source encodes");
        serialized_source
            .as_object_mut()
            .expect("workspace source is an object")
            .insert(
                "provider_product".to_owned(),
                serde_json::json!({"resolution":"applied"}),
            );
        assert!(serde_json::from_value::<AgentWorkspaceSource>(serialized_source).is_err());

        let mut settle_request = fixture.request;
        settle_request.dispatch_lease = None;
        let settle = AgentWorkspaceCommand::SettleEffect {
            request: settle_request,
        };
        let settle_command = AgentCommand::new(
            revision('4'),
            AgentCommandAction::Workspace(Box::new(settle)),
        )
        .expect("workspace settlement command seals");
        let mut serialized_command =
            serde_json::to_value(&settle_command).expect("workspace command encodes");
        serialized_command["action"]["command"]
            .as_object_mut()
            .expect("workspace command is a tagged object")
            .insert(
                "resolution".to_owned(),
                serde_json::json!({"resolution":"applied"}),
            );
        assert!(serde_json::from_value::<AgentCommand>(serialized_command).is_err());
    }

    #[test]
    fn workspace_provider_preflight_rejects_before_io() {
        let fixture = workspace_test_fixture();
        let mut settle_request = fixture.request;
        settle_request.dispatch_lease = None;
        let settle = AgentWorkspaceCommand::SettleEffect {
            request: settle_request,
        };
        let settle_command = AgentCommand::new(
            revision('4'),
            AgentCommandAction::Workspace(Box::new(settle)),
        )
        .expect("workspace settlement command seals");
        let mut providers = TestAgentProviders {
            workspace_resolution: Some(AgentOccurrenceResolution::Unknown {
                evidence: Vec::new(),
            }),
            ..TestAgentProviders::default()
        };
        assert!(
            execute_agent_workspace_provider(&fixture.source, &settle_command, &mut providers)
                .is_err()
        );
        assert_eq!(providers.observation_calls, 0);
    }

    #[test]
    fn oversized_values_and_pages_fail_at_the_profile_boundary() {
        let oversized_plan = AgentPlan {
            plan_id: "plan:oversized".to_owned(),
            entries: (0..=MAX_AGENT_VALUE_ENTRIES)
                .map(|index| AgentPlanEntry {
                    entry_id: format!("entry:{index}"),
                    content: "x".to_owned(),
                    status: PlanEntryStatus::Pending,
                })
                .collect(),
        };
        assert!(
            AgentUpdate::Plan {
                update_id: "update:oversized-plan".to_owned(),
                plan: oversized_plan,
            }
            .validate_content()
            .is_err()
        );
        assert!(
            message_update(
                "update:oversized-message",
                "message:oversized",
                "x".repeat(MAX_AGENT_VALUE_BYTES + 1),
            )
            .validate_content()
            .is_err()
        );
        assert!(
            AgentHostOccurrence::prepare(
                "occurrence:oversized",
                "session:oversized",
                AgentHostRequest::Tool(ToolRequest {
                    tool_call_id: "tool:oversized".to_owned(),
                    operation: "tool.large".to_owned(),
                    input: Value::String("x".repeat(MAX_AGENT_VALUE_BYTES + 1)),
                }),
                AgentHostBinding::standalone("tool:test/1", "binding:tool/1")
                    .expect("binding constructs"),
            )
            .is_err()
        );

        let mut session =
            AgentSessionCurrent::new("session:page-bytes").expect("Session constructs");
        let mut entries = Vec::new();
        for index in 0..24_u64 {
            let update = message_update(
                &format!("update:page:{index}"),
                &format!("message:page:{index}"),
                "x".repeat(200_000),
            );
            let post = session
                .reduce_update(
                    &content_id("test.agent-command/1", &index).expect("command id derives"),
                    &update,
                    &AgentSessionUpdateSource {
                        update: None,
                        entry: AgentSessionEntrySource::Message { current: None },
                        target_claims: absent_message_claim(&format!("message:page:{index}")),
                    },
                )
                .expect("bounded message admits");
            let AgentSessionUpdateEffect::Message { current } = post.effect else {
                panic!("message effect shape")
            };
            entries.push(current);
            session = post.session;
        }
        let page = AgentMessagePage {
            session_id: session.session_id,
            expected_message_head: session.message_head,
            source_message_count: session.message_count,
            end_exclusive: Some(24),
            entries,
            next_end_exclusive: None,
        };
        assert!(page.verify().is_err());
    }

    #[test]
    fn typed_reads_bind_revision_key_cursor_and_response_budget() {
        let query = AgentSessionQuery {
            session_id: "session:read".to_owned(),
            expected_revision: Some(revision('1')),
        };
        AgentSessionRead {
            revision: revision('1'),
            current: None,
        }
        .verify_for(&query)
        .expect("absent exact Session read remains revision pinned");
        assert!(
            AgentSessionRead {
                revision: revision('2'),
                current: None,
            }
            .verify_for(&query)
            .is_err()
        );
        let explicit_null = serde_json::json!({
            "revision": revision('1'),
            "current": null
        });
        serde_json::from_value::<AgentSessionRead>(explicit_null.clone())
            .expect("explicit-null exact result decodes");
        let mut missing = explicit_null;
        missing
            .as_object_mut()
            .expect("read fixture is an object")
            .remove("current");
        assert!(serde_json::from_value::<AgentSessionRead>(missing).is_err());

        let page_query = AgentMessagePageQuery {
            session_id: "session:read".to_owned(),
            expected_message_head: None,
            source_message_count: 0,
            end_exclusive: None,
            max_entries: 1,
            max_message_canonical_bytes: 1024,
            max_canonical_bytes: 1024,
            expected_revision: Some(revision('1')),
        };
        let read = AgentMessagePageRead {
            revision: revision('1'),
            page: AgentMessagePage {
                session_id: "session:read".to_owned(),
                expected_message_head: None,
                source_message_count: 0,
                end_exclusive: None,
                entries: Vec::new(),
                next_end_exclusive: None,
            },
        };
        read.verify_for(&page_query)
            .expect("empty bounded page remains revision and cursor pinned");
        let mut undersized_budget = page_query;
        undersized_budget.max_canonical_bytes = 1;
        assert!(read.verify_for(&undersized_budget).is_err());

        assert!(
            AgentMessagePage {
                session_id: "session:read".to_owned(),
                expected_message_head: Some(revision('2')),
                source_message_count: 1,
                end_exclusive: Some(1),
                entries: Vec::new(),
                next_end_exclusive: None,
            }
            .verify()
            .is_err()
        );
    }

    #[test]
    fn message_page_source_and_dual_byte_budgets_are_exact() {
        let session = AgentSessionCurrent::new("session:page-budget").expect("Session constructs");
        let update = message_update(
            "update:page-budget",
            "message:page-budget",
            "bounded page message".to_owned(),
        );
        let postcondition = session
            .reduce_update(
                &revision('1'),
                &update,
                &AgentSessionUpdateSource {
                    update: None,
                    entry: AgentSessionEntrySource::Message { current: None },
                    target_claims: absent_message_claim("message:page-budget"),
                },
            )
            .expect("message enters the source prefix");
        let AgentSessionUpdateEffect::Message { current } = postcondition.effect else {
            panic!("message update returns its exact current")
        };
        let message_bytes = message_page_entry_canonical_bytes(std::slice::from_ref(&current))
            .expect("message-current bytes sum");
        let read = AgentMessagePageRead {
            revision: revision('2'),
            page: AgentMessagePage {
                session_id: postcondition.session.session_id.clone(),
                expected_message_head: postcondition.session.message_head.clone(),
                source_message_count: postcondition.session.message_count,
                end_exclusive: None,
                entries: vec![current],
                next_end_exclusive: None,
            },
        };
        let wire_bytes = cymule_core::canonical_bytes(&read)
            .expect("message page read encodes")
            .len();
        let query = AgentMessagePageQuery {
            session_id: postcondition.session.session_id,
            expected_message_head: postcondition.session.message_head,
            source_message_count: postcondition.session.message_count,
            end_exclusive: None,
            max_entries: 1,
            max_message_canonical_bytes: message_bytes as u64,
            max_canonical_bytes: wire_bytes as u64,
            expected_revision: Some(revision('2')),
        };
        read.verify_for(&query)
            .expect("both exact page byte budgets verify independently");

        let mut undersized_messages = query.clone();
        undersized_messages.max_message_canonical_bytes -= 1;
        assert!(read.verify_for(&undersized_messages).is_err());
        let mut undersized_wire = query.clone();
        undersized_wire.max_canonical_bytes -= 1;
        assert!(read.verify_for(&undersized_wire).is_err());

        let mut wrong_count = read.clone();
        wrong_count.page.source_message_count += 1;
        assert!(read.page.verify().is_ok());
        assert!(wrong_count.verify_for(&query).is_err());

        let query_wire = serde_json::to_value(&query).expect("message page query encodes");
        for field in ["source_message_count", "max_message_canonical_bytes"] {
            for invalid in [None, Some(Value::Null), Some(Value::String("1".to_owned()))] {
                let mut wire = query_wire.clone();
                match invalid {
                    None => {
                        wire.as_object_mut()
                            .expect("message page query is an object")
                            .remove(field);
                    }
                    Some(value) => wire[field] = value,
                }
                assert!(serde_json::from_value::<AgentMessagePageQuery>(wire).is_err());
            }
        }
        let page_wire = serde_json::to_value(&read.page).expect("message page encodes");
        for invalid in [None, Some(Value::Null), Some(Value::String("1".to_owned()))] {
            let mut wire = page_wire.clone();
            match invalid {
                None => {
                    wire.as_object_mut()
                        .expect("message page is an object")
                        .remove("source_message_count");
                }
                Some(value) => wire["source_message_count"] = value,
            }
            assert!(serde_json::from_value::<AgentMessagePage>(wire).is_err());
        }

        let mut invalid_query = query;
        invalid_query.source_message_count = MAX_EXACT_INTEGER + 1;
        assert!(invalid_query.verify().is_err());
        invalid_query.source_message_count = 1;
        invalid_query.max_message_canonical_bytes = 0;
        assert!(invalid_query.verify().is_err());
        invalid_query.max_message_canonical_bytes = MAX_AGENT_PAGE_BYTES as u64 + 1;
        assert!(invalid_query.verify().is_err());
    }

    #[test]
    fn persisted_unions_reject_unknown_members() {
        assert!(
            serde_json::from_value::<ContentBlock>(
                serde_json::json!({"type":"text","text":"x","unknown":true})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<AgentUpdate>(serde_json::json!({
                "type":"state","update_id":"update:unknown","state":"running",
                "stop_reason":null,"unknown":true
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<AgentHostRequest>(serde_json::json!({
                "kind":"context",
                "request":{
                    "session_id":"session:unknown",
                    "source_message_head":null,
                    "source_message_count":0,
                    "budget":1,
                    "scan_limits":{"max_entries":1,"max_canonical_bytes":1}
                },
                "unknown":true
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<AgentHostResponse>(serde_json::json!({
                "kind":"context",
                "response":{
                    "snapshot_id":"snapshot:unknown",
                    "source_message_head":null,
                    "source_message_count":0,
                    "selected_messages":[],
                    "content":[],
                    "occurrence_binding":"binding:unknown/1"
                },
                "unknown":true
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<AgentOccurrenceResolution>(serde_json::json!({
                "resolution":"unknown",
                "evidence":[],
                "unknown":true
            }))
            .is_err()
        );
    }

    #[test]
    fn permission_response_must_select_one_offered_typed_decision() {
        let request = AgentHostRequest::Permission(PermissionRequest {
            request_id: "permission:closed".to_owned(),
            tool: ToolRequest {
                tool_call_id: "tool:closed".to_owned(),
                operation: "tool.read".to_owned(),
                input: serde_json::json!({}),
            },
            options: vec![PermissionDecision::Deny],
        });
        request
            .validate_for_session("session:closed")
            .expect("one typed decision is a valid closed request");
        validate_response_for_request(
            &request,
            &AgentHostResponse::Permission(PermissionResponse {
                decision: PermissionDecision::Deny,
                occurrence_binding: "binding:permission/1".to_owned(),
            }),
        )
        .expect("an offered decision is accepted");
        assert!(
            validate_response_for_request(
                &request,
                &AgentHostResponse::Permission(PermissionResponse {
                    decision: PermissionDecision::AllowOnce,
                    occurrence_binding: "binding:permission/1".to_owned(),
                }),
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<PermissionRequest>(serde_json::json!({
                "request_id": "permission:open-string",
                "tool": {
                    "tool_call_id": "tool:open-string",
                    "operation": "tool.read",
                    "input": {}
                },
                "options": ["allow_forever"]
            }))
            .is_err()
        );
    }

    #[test]
    fn context_response_keeps_the_exact_pinned_message_descriptor() {
        let source_message_head = revision('a');
        let request = AgentHostRequest::Context(ContextRequest {
            session_id: "session:context-head".to_owned(),
            source_message_head: Some(source_message_head.clone()),
            source_message_count: 1,
            budget: 1,
            scan_limits: AgentContextScanLimits {
                max_entries: 1,
                max_canonical_bytes: 1024,
            },
        });
        let response = AgentHostResponse::Context(ContextSnapshot {
            snapshot_id: "snapshot:context-head".to_owned(),
            source_message_head: Some(revision('b')),
            source_message_count: 2,
            selected_messages: Vec::new(),
            content: Vec::new(),
            occurrence_binding: "binding:context-head/1".to_owned(),
        });

        assert!(validate_response_for_request(&request, &response).is_err());
        let AgentHostResponse::Context(mut response) = response else {
            panic!("fixture is a context response");
        };
        response.source_message_head = Some(source_message_head);
        assert!(
            validate_response_for_request(&request, &AgentHostResponse::Context(response.clone()))
                .is_err()
        );
        response.source_message_count = 1;
        validate_response_for_request(&request, &AgentHostResponse::Context(response))
            .expect("context response retains its request source descriptor");
    }

    #[test]
    fn context_message_binding_exactly_matches_its_persisted_current() {
        let session = AgentSessionCurrent::new("session:context-message")
            .expect("Session current constructs");
        let update = message_update(
            "update:context-message",
            "message:context-message",
            "persisted context".to_owned(),
        );
        let postcondition = session
            .reduce_update(
                &revision('c'),
                &update,
                &AgentSessionUpdateSource {
                    update: None,
                    entry: AgentSessionEntrySource::Message { current: None },
                    target_claims: absent_message_claim("message:context-message"),
                },
            )
            .expect("message update reduces");
        let AgentSessionUpdateEffect::Message { current } = postcondition.effect else {
            panic!("message update has one message effect")
        };
        let selected = AgentContextMessageRef::from_current(&current)
            .expect("the selection reference derives from the exact current");
        selected
            .verify_for(&current)
            .expect("exact message binding verifies");

        let mut forged = selected.clone();
        forged.message_digest = revision('d');
        assert!(forged.verify_for(&current).is_err());
        forged = selected.clone();
        forged.message_id = "message:other".to_owned();
        assert!(forged.verify_for(&current).is_err());
        forged = selected;
        forged.index += 1;
        assert!(forged.verify_for(&current).is_err());
    }
}
