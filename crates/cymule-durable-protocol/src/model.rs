//! Closed wire values whose semantic owner is the durable protocol.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
};

use cymule_core::{ArtifactRef, InvocationPathSegment, MAX_EXACT_INTEGER, content_id};
use serde::{Deserialize, Serialize};

use crate::{DurableProtocolError, DurableProtocolResult};

/// Frozen logical Clock observation receipt version.
pub const CLOCK_OBSERVATION_VERSION: &str = "cymule.clock-observation/2";
/// Durable continuation execution-claim version.
pub const EXECUTION_CLAIM_VERSION: &str = "cymule.continuation-execution-claim/1";
/// Complete Continuation state DTO generation.
pub const CONTINUATION_STATE_VERSION: &str = "cymule.continuation-state/1";
/// Identified external wait activation version.
pub const WAIT_ACTIVATION_VERSION: &str = "cymule.wait-activation/2";
/// Durable wait activation receipt version.
pub const WAIT_ACTIVATION_RECEIPT_VERSION: &str = "cymule.wait-activation-receipt/3";
/// Canonical Artifact kind for one direct typed wait-completion result.
pub const WAIT_RESULT_ARTIFACT_KIND: &str = "cymule.wait-result/1";
/// Maximum targets admitted by one identified wait activation.
pub const MAX_WAIT_DELIVERY_TARGETS: usize = 4_096;
/// Maximum raw ingress or compact serialized JSON bytes for one Continuation.
pub const MAX_CONTINUATION_WIRE_BYTES: usize = 4 * 1024 * 1024;
/// Maximum interpreter frames retained by one Continuation.
pub const MAX_CONTINUATION_FRAMES: usize = 1_024;
/// Maximum active wait identities retained by one Continuation.
pub const MAX_CONTINUATION_WAIT_IDS: usize = 4_096;
/// Maximum lexical scope depth retained by one Continuation.
pub const MAX_CONTINUATION_SCOPE_DEPTH: usize = 1_024;
/// Maximum dynamic invocation depth retained by one frame.
pub const MAX_FRAME_INVOCATION_DEPTH: usize = 1_024;
/// Maximum structural Region nesting retained in any one path.
pub const MAX_REGION_PATH_DEPTH: usize = 256;
/// Maximum local Artifact bindings retained by one frame.
pub const MAX_FRAME_LOCALS: usize = 4_096;
/// Maximum aggregate collection entries retained by one Continuation.
pub const MAX_CONTINUATION_AGGREGATE_ITEMS: usize = 16_384;
/// Maximum aggregate Unicode identity scalars retained by one Continuation.
pub const MAX_CONTINUATION_IDENTITY_SCALARS: usize = 262_144;

const EXECUTION_CLOCK_SCOPE_ID_DOMAIN: &str = "cymule.execution-clock-scope/1";
const CONTINUATION_ID_DOMAIN: &str = "cymule.continuation/1";

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ContinuationIdPreimage<'a> {
    run_id: &'a str,
}

/// Immutable identity of one retained logical Clock observation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockObservationRef {
    /// Frozen receipt version.
    pub clock_version: String,
    /// Content identity of the complete retained receipt.
    pub observation_id: String,
    /// Stable configured Clock source.
    pub source_id: String,
    /// Immutable source implementation/configuration generation.
    pub source_generation: String,
    /// Independent monotonic Clock scope.
    pub scope: String,
}

impl ClockObservationRef {
    /// Validate the closed reference shape.
    ///
    /// # Errors
    ///
    /// Returns an error when the version or any identity field is malformed.
    pub fn verify(&self) -> DurableProtocolResult<()> {
        if self.clock_version != CLOCK_OBSERVATION_VERSION {
            return Err(DurableProtocolError::Validation(format!(
                "unsupported Clock observation reference version {}",
                self.clock_version
            )));
        }
        validate_sha256("Clock observation", &self.observation_id)?;
        validate_identity("Clock source", &self.source_id)?;
        validate_sha256("Clock source generation", &self.source_generation)?;
        validate_identity("Clock scope", &self.scope)
    }
}

/// Complete self-authenticating logical Clock receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockObservation {
    /// Frozen receipt version.
    pub clock_version: String,
    /// Content identity of this complete receipt.
    pub observation_id: String,
    /// Stable configured source identity.
    pub source_id: String,
    /// Immutable source implementation/configuration generation.
    pub source_generation: String,
    /// Independent monotonic Clock scope.
    pub scope: String,
    /// Strictly increasing logical value allocated by the source.
    pub logical_time: u64,
    /// Non-authoritative wall time sampled during allocation.
    pub observed_unix_ms: u64,
}

impl ClockObservation {
    /// Borrow the immutable receipt reference.
    pub fn reference(&self) -> ClockObservationRef {
        ClockObservationRef {
            clock_version: self.clock_version.clone(),
            observation_id: self.observation_id.clone(),
            source_id: self.source_id.clone(),
            source_generation: self.source_generation.clone(),
            scope: self.scope.clone(),
        }
    }

    /// Verify the complete content identity and closed integer bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when the receipt is malformed, exceeds the exact
    /// integer range, or does not match its content-derived identity.
    pub fn verify(&self) -> DurableProtocolResult<()> {
        self.reference().verify()?;
        if self.logical_time > MAX_EXACT_INTEGER || self.observed_unix_ms > MAX_EXACT_INTEGER {
            return Err(DurableProtocolError::Validation(
                "Clock observation exceeds the exact cross-language integer range".to_owned(),
            ));
        }
        let expected = clock_observation_id(
            &self.source_id,
            &self.source_generation,
            &self.scope,
            self.logical_time,
            self.observed_unix_ms,
        )?;
        if self.observation_id != expected {
            return Err(DurableProtocolError::IdentityMismatch(
                "Clock observation identity does not match its content".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Derive the canonical identity of one substrate-issued Clock receipt.
///
/// # Errors
///
/// Returns an error when an identity is malformed, a counter exceeds the
/// exact integer range, or canonical identity derivation fails.
pub fn clock_observation_id(
    source_id: &str,
    source_generation: &str,
    scope: &str,
    logical_time: u64,
    observed_unix_ms: u64,
) -> DurableProtocolResult<String> {
    validate_identity("Clock source", source_id)?;
    validate_sha256("Clock source generation", source_generation)?;
    validate_identity("Clock scope", scope)?;
    if logical_time > MAX_EXACT_INTEGER || observed_unix_ms > MAX_EXACT_INTEGER {
        return Err(DurableProtocolError::Validation(
            "Clock observation exceeds the exact cross-language integer range".to_owned(),
        ));
    }
    content_id(
        CLOCK_OBSERVATION_VERSION,
        &(
            source_id,
            source_generation,
            scope,
            logical_time,
            observed_unix_ms,
        ),
    )
    .map_err(Into::into)
}

/// Exact Clock scope used by one Run's execution ownership.
///
/// # Errors
///
/// Returns an error when the Run identity is malformed or canonical identity
/// derivation fails.
pub fn execution_clock_scope(run_id: &str) -> DurableProtocolResult<String> {
    validate_identity("Run", run_id)?;
    content_id(EXECUTION_CLOCK_SCOPE_ID_DOMAIN, &run_id).map_err(Into::into)
}

/// Exact authority input for one execution-claim acquisition or takeover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionClaimRequest {
    /// Exact driver identity for this execution command.
    pub owner: String,
    /// Retained logical Clock receipt reference.
    pub clock: ClockObservationRef,
    /// Positive logical claim duration.
    pub ttl: u64,
}

impl ExecutionClaimRequest {
    /// Validate the request independently of durable state.
    ///
    /// # Errors
    ///
    /// Returns an error when the owner, Clock reference, or TTL is malformed.
    pub fn verify(&self) -> DurableProtocolResult<()> {
        validate_identity("execution owner", &self.owner)?;
        if self.ttl == 0 || self.ttl > MAX_EXACT_INTEGER {
            return Err(DurableProtocolError::Validation(
                "execution claim TTL must use the exact positive cross-language range".to_owned(),
            ));
        }
        self.clock.verify()
    }
}

/// Complete versioned effectful continuation at a semantic safe point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Continuation {
    /// Complete Continuation state DTO generation.
    pub continuation_version: String,
    /// Stable owning Run identity.
    pub run_id: String,
    /// Current immutable Plan identity.
    pub plan_id: String,
    /// Future-default execution binding context.
    pub binding_context: String,
    /// Logical interpreter stack.
    pub frames: Vec<FrameState>,
    /// Current typed state Artifact, when present.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub state: Option<ArtifactRef>,
    /// Active durable wait identities.
    pub wait_set: BTreeSet<String>,
    /// Open lexical scopes from root to current.
    pub scope_stack: Vec<String>,
    /// Attempt fencing epoch.
    pub epoch: u64,
    /// Monotonic execution-claim fence.
    pub execution_fence: u64,
    /// Active single-driver claim, present exactly while running.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub execution_claim: Option<ContinuationExecutionClaim>,
    /// Current lifecycle state.
    pub status: ContinuationStatus,
}

/// One durable single-driver claim for a running Continuation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationExecutionClaim {
    /// Frozen claim version.
    pub claim_version: String,
    /// Stable owning Run identity.
    pub run_id: String,
    /// Stable Run-owned Continuation identity derived from the Run ID.
    pub continuation_id: String,
    /// Exact driver identity.
    pub owner: String,
    /// Current Continuation attempt authorized by this claim.
    pub continuation_attempt_id: String,
    /// Monotonic claim fence.
    pub fence: u64,
    /// Exact semantic Plan interpreted by the driver.
    pub plan_id: String,
    /// Exact execution-binding Artifact interpreted by the driver.
    pub execution_binding_ref: ArtifactRef,
    /// Retained Clock receipt authorizing acquisition.
    pub clock_observation_ref: ClockObservationRef,
    /// Verified logical acquisition value.
    pub logical_acquired_at: u64,
    /// Positive logical claim duration.
    pub logical_ttl: u64,
    /// Verified logical expiry value.
    pub logical_expires_at: u64,
}

/// Logical interpreter frame without process-memory references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameState {
    /// Definition resolved inside the immutable Plan.
    pub definition_id: String,
    /// Structural materialized invocation identity.
    pub invocation_id: String,
    /// Entry-rooted dynamic invocation path.
    pub invocation_path: Vec<InvocationPathSegment>,
    /// Exact lexical scope owning this frame.
    pub scope_id: String,
    /// Typed invocation input Artifact.
    pub input: ArtifactRef,
    /// Nested Region indices from the definition root.
    pub region_path: Vec<usize>,
    /// Next stable step index.
    pub next_step: usize,
    /// Typed local bindings stored as immutable Artifacts.
    pub locals: BTreeMap<String, ArtifactRef>,
}

/// Continuation lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationStatus {
    /// Ready for a fenced attempt.
    Ready,
    /// Blocked on durable waits.
    Waiting,
    /// Held by one active execution claim.
    Running,
    /// Terminal success.
    Completed,
    /// Terminal failure.
    Failed,
    /// Terminal cancellation.
    Cancelled,
}

impl Continuation {
    /// Decode one untrusted Continuation under the protocol byte bound and
    /// validate every closed wire invariant.
    ///
    /// # Errors
    ///
    /// Returns an error before decoding when the input exceeds the fixed byte
    /// bound, when JSON decoding fails, or when wire verification fails.
    pub fn decode_strict(bytes: &[u8]) -> DurableProtocolResult<Self> {
        if bytes.len() > MAX_CONTINUATION_WIRE_BYTES {
            return Err(DurableProtocolError::Validation(format!(
                "Continuation exceeds {MAX_CONTINUATION_WIRE_BYTES} wire bytes"
            )));
        }
        let continuation: Self = cymule_core::decode_json(bytes)?;
        continuation.verify_wire()?;
        Ok(continuation)
    }

    /// Validate all self-contained wire invariants.
    ///
    /// # Errors
    ///
    /// Returns an error when any identity, counter, frame, state, lifecycle,
    /// wait, scope, or execution-claim invariant is violated.
    pub fn verify_wire(&self) -> DurableProtocolResult<()> {
        self.verify_shape_bounds()?;
        if self.continuation_version != CONTINUATION_STATE_VERSION {
            return Err(DurableProtocolError::Validation(format!(
                "unsupported Continuation state version {:?}",
                self.continuation_version
            )));
        }
        validate_identity("Continuation Run", &self.run_id)?;
        validate_identity("Continuation Plan", &self.plan_id)?;
        validate_identity("Continuation binding", &self.binding_context)?;
        if self.epoch > MAX_EXACT_INTEGER || self.execution_fence > MAX_EXACT_INTEGER {
            return Err(DurableProtocolError::Validation(
                "Continuation counter exceeds the exact integer range".to_owned(),
            ));
        }
        if matches!(
            self.status,
            ContinuationStatus::Ready | ContinuationStatus::Waiting | ContinuationStatus::Running
        ) && (self.frames.is_empty() || self.scope_stack.is_empty())
        {
            return Err(DurableProtocolError::Validation(
                "active Continuation requires a frame and scope stack".to_owned(),
            ));
        }
        for identity in self.wait_set.iter().chain(&self.scope_stack) {
            validate_identity("Continuation reference", identity)?;
        }
        if (self.status == ContinuationStatus::Waiting) == self.wait_set.is_empty() {
            return Err(DurableProtocolError::Validation(
                "Continuation waiting status does not match its wait set".to_owned(),
            ));
        }
        if let Some(state) = &self.state {
            state.validate().map_err(|error| {
                DurableProtocolError::Validation(format!("Continuation state is invalid: {error}"))
            })?;
        }
        for frame in &self.frames {
            validate_identity("frame definition", &frame.definition_id)?;
            validate_identity("frame invocation", &frame.invocation_id)?;
            validate_identity("frame scope", &frame.scope_id)?;
            frame.input.validate().map_err(|error| {
                DurableProtocolError::Validation(format!("frame input is invalid: {error}"))
            })?;
            validate_indices("frame Region path", &frame.region_path)?;
            validate_exact_usize("frame step", frame.next_step)?;
            for (name, local) in &frame.locals {
                validate_identity("frame local", name)?;
                local.validate().map_err(|error| {
                    DurableProtocolError::Validation(format!("frame local is invalid: {error}"))
                })?;
            }
            for segment in &frame.invocation_path {
                validate_identity("invocation site", &segment.site_id)?;
                validate_identity("invocation scope", &segment.scope_id)?;
                validate_indices("invocation Region path", &segment.region_path)?;
            }
        }
        match (&self.status, &self.execution_claim) {
            (ContinuationStatus::Running, Some(claim)) => claim.verify_wire(self),
            (ContinuationStatus::Running, None) => Err(DurableProtocolError::Validation(
                "running Continuation requires an execution claim".to_owned(),
            )),
            (_, Some(_)) => Err(DurableProtocolError::Validation(
                "non-running Continuation cannot retain an execution claim".to_owned(),
            )),
            (_, None) => Ok(()),
        }
    }

    fn verify_shape_bounds(&self) -> DurableProtocolResult<()> {
        validate_count(
            "Continuation frames",
            self.frames.len(),
            MAX_CONTINUATION_FRAMES,
        )?;
        validate_count(
            "Continuation wait identities",
            self.wait_set.len(),
            MAX_CONTINUATION_WAIT_IDS,
        )?;
        validate_count(
            "Continuation scope depth",
            self.scope_stack.len(),
            MAX_CONTINUATION_SCOPE_DEPTH,
        )?;

        let mut aggregate_items = 0_usize;
        let mut identity_scalars = 0_usize;
        account_items(
            &mut aggregate_items,
            self.frames.len(),
            "Continuation aggregate items",
        )?;
        account_items(
            &mut aggregate_items,
            self.wait_set.len(),
            "Continuation aggregate items",
        )?;
        account_items(
            &mut aggregate_items,
            self.scope_stack.len(),
            "Continuation aggregate items",
        )?;
        for identity in [
            &self.continuation_version,
            &self.run_id,
            &self.plan_id,
            &self.binding_context,
        ] {
            account_identity_scalars(&mut identity_scalars, identity)?;
        }
        if let Some(state) = &self.state {
            account_artifact_ref_identity_scalars(&mut identity_scalars, state)?;
        }
        for identity in self.wait_set.iter().chain(&self.scope_stack) {
            account_identity_scalars(&mut identity_scalars, identity)?;
        }
        if let Some(claim) = &self.execution_claim {
            account_execution_claim_identity_scalars(&mut identity_scalars, claim)?;
        }

        for frame in &self.frames {
            validate_count(
                "frame invocation depth",
                frame.invocation_path.len(),
                MAX_FRAME_INVOCATION_DEPTH,
            )?;
            validate_count(
                "frame Region depth",
                frame.region_path.len(),
                MAX_REGION_PATH_DEPTH,
            )?;
            validate_count("frame locals", frame.locals.len(), MAX_FRAME_LOCALS)?;
            account_items(
                &mut aggregate_items,
                frame.invocation_path.len(),
                "Continuation aggregate items",
            )?;
            account_items(
                &mut aggregate_items,
                frame.region_path.len(),
                "Continuation aggregate items",
            )?;
            account_items(
                &mut aggregate_items,
                frame.locals.len(),
                "Continuation aggregate items",
            )?;
            for identity in [&frame.definition_id, &frame.invocation_id, &frame.scope_id] {
                account_identity_scalars(&mut identity_scalars, identity)?;
            }
            account_artifact_ref_identity_scalars(&mut identity_scalars, &frame.input)?;
            for (name, local) in &frame.locals {
                account_identity_scalars(&mut identity_scalars, name)?;
                account_artifact_ref_identity_scalars(&mut identity_scalars, local)?;
            }
            for segment in &frame.invocation_path {
                validate_count(
                    "invocation Region depth",
                    segment.region_path.len(),
                    MAX_REGION_PATH_DEPTH,
                )?;
                account_items(
                    &mut aggregate_items,
                    segment.region_path.len(),
                    "Continuation aggregate items",
                )?;
                account_identity_scalars(&mut identity_scalars, &segment.site_id)?;
                account_identity_scalars(&mut identity_scalars, &segment.scope_id)?;
            }
        }
        verify_compact_wire_bound("Continuation", self, MAX_CONTINUATION_WIRE_BYTES)
    }
}

impl ContinuationExecutionClaim {
    /// Validate the claim against its owning Continuation.
    ///
    /// # Errors
    ///
    /// Returns an error when the claim is malformed or does not exactly match
    /// the owning Continuation.
    pub fn verify_wire(&self, continuation: &Continuation) -> DurableProtocolResult<()> {
        validate_identity("Continuation Attempt", &self.continuation_attempt_id)?;
        self.execution_binding_ref.validate().map_err(|error| {
            DurableProtocolError::Validation(format!("execution binding is invalid: {error}"))
        })?;
        ExecutionClaimRequest {
            owner: self.owner.clone(),
            clock: self.clock_observation_ref.clone(),
            ttl: self.logical_ttl,
        }
        .verify()?;
        if self.claim_version != EXECUTION_CLAIM_VERSION
            || self.run_id != continuation.run_id
            || self.continuation_id != continuation_id(&continuation.run_id)?
            || self.fence == 0
            || self.fence > MAX_EXACT_INTEGER
            || self.fence != continuation.execution_fence
            || self.plan_id != continuation.plan_id
            || self.execution_binding_ref.artifact_id != continuation.binding_context
            || self.execution_binding_ref.kind != cymule_core::EXECUTION_BINDING_ARTIFACT_KIND
            || self.execution_binding_ref.identity_version != cymule_core::ARTIFACT_IDENTITY_VERSION
            || self.clock_observation_ref.scope != execution_clock_scope(&continuation.run_id)?
            || self.logical_acquired_at > MAX_EXACT_INTEGER
            || self.logical_acquired_at.checked_add(self.logical_ttl)
                != Some(self.logical_expires_at)
            || self.logical_expires_at > MAX_EXACT_INTEGER
        {
            return Err(DurableProtocolError::Validation(format!(
                "Continuation {} execution claim is inconsistent",
                continuation.run_id
            )));
        }
        Ok(())
    }

    /// Validate the claim against its Continuation and retained Clock table.
    ///
    /// # Errors
    ///
    /// Returns an error when wire validation fails, the exact retained Clock
    /// receipt is absent, or receipt content does not authorize the claim.
    pub fn verify(
        &self,
        continuation: &Continuation,
        observations: &BTreeMap<String, ClockObservation>,
    ) -> DurableProtocolResult<()> {
        self.verify_wire(continuation)?;
        let observation = observations
            .get(&self.clock_observation_ref.observation_id)
            .ok_or_else(|| {
                DurableProtocolError::Validation(format!(
                    "execution claim {} references missing Clock observation",
                    self.continuation_attempt_id
                ))
            })?;
        observation.verify()?;
        if observation.reference() != self.clock_observation_ref
            || observation.scope != execution_clock_scope(&continuation.run_id)?
            || observation.logical_time != self.logical_acquired_at
            || observation.logical_time.checked_add(self.logical_ttl)
                != Some(self.logical_expires_at)
            || self.logical_expires_at > MAX_EXACT_INTEGER
        {
            return Err(DurableProtocolError::Validation(format!(
                "execution claim {} does not match its Clock observation",
                self.continuation_attempt_id
            )));
        }
        Ok(())
    }
}

/// Exact Plan frame and operation site owning one durable wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitOwner {
    /// Structural invocation owning the local.
    pub invocation_id: String,
    /// Definition containing the wait site.
    pub definition_id: String,
    /// Stable wait operation site.
    pub site_id: String,
    /// Nested Region path within the definition.
    pub region_path: Vec<usize>,
    /// Wait step index within the Region.
    pub step_index: usize,
    /// Optional local binding declared by the wait operation.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub bind: Option<String>,
}

impl WaitOwner {
    /// Validate the complete structural owner.
    ///
    /// # Errors
    ///
    /// Returns an error when an identity or exact structural index is invalid.
    pub fn verify(&self) -> DurableProtocolResult<()> {
        validate_identity("wait owner invocation", &self.invocation_id)?;
        validate_identity("wait owner definition", &self.definition_id)?;
        validate_identity("wait owner site", &self.site_id)?;
        validate_count(
            "wait owner Region depth",
            self.region_path.len(),
            MAX_REGION_PATH_DEPTH,
        )?;
        validate_indices("wait owner Region path", &self.region_path)?;
        validate_exact_usize("wait owner step", self.step_index)?;
        if let Some(bind) = &self.bind {
            validate_identity("wait owner bind", bind)?;
        }
        Ok(())
    }
}

/// One externally identified signal or timer delivery admitted by M1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitActivation {
    /// Activation schema and semantic version.
    pub activation_version: String,
    /// Stable external delivery identity.
    pub activation_id: String,
    /// Expected signal or timer source.
    pub source: WaitActivationSource,
    /// Exact pending waits selected by the scheduler.
    pub wait_ids: BTreeSet<String>,
    /// Immutable typed completion result shared by selected waits.
    pub result: ArtifactRef,
}

impl WaitActivation {
    /// Construct and validate an identified wait activation.
    ///
    /// # Errors
    ///
    /// Returns an error when the activation identity, source, target set, or
    /// result Artifact reference is invalid.
    pub fn new(
        activation_id: impl Into<String>,
        source: WaitActivationSource,
        wait_ids: BTreeSet<String>,
        result: ArtifactRef,
    ) -> DurableProtocolResult<Self> {
        let activation = Self {
            activation_version: WAIT_ACTIVATION_VERSION.to_owned(),
            activation_id: activation_id.into(),
            source,
            wait_ids,
            result,
        };
        activation.verify()?;
        Ok(activation)
    }

    /// Validate the versioned activation independently of durable state.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, identity, target set, result, or
    /// source is invalid.
    pub fn verify(&self) -> DurableProtocolResult<()> {
        if self.activation_version != WAIT_ACTIVATION_VERSION {
            return Err(DurableProtocolError::Validation(format!(
                "unsupported wait activation version {:?}",
                self.activation_version
            )));
        }
        validate_identity("wait activation", &self.activation_id)?;
        if self.wait_ids.is_empty() || self.wait_ids.len() > MAX_WAIT_DELIVERY_TARGETS {
            return Err(DurableProtocolError::Validation(format!(
                "wait activation must target 1..={MAX_WAIT_DELIVERY_TARGETS} waits"
            )));
        }
        for wait_id in &self.wait_ids {
            validate_sha256("wait activation target", wait_id)?;
        }
        self.result.validate().map_err(|error| {
            DurableProtocolError::Validation(format!("wait activation result is invalid: {error}"))
        })?;
        if self.result.kind != WAIT_RESULT_ARTIFACT_KIND {
            return Err(DurableProtocolError::Validation(format!(
                "wait activation result must use Artifact kind {WAIT_RESULT_ARTIFACT_KIND}"
            )));
        }
        self.source.verify()
    }
}

/// Aggregate disposition of one identified wait activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitActivationDisposition {
    /// At least one selected wait was completed.
    Applied,
    /// Every selected wait was already terminal.
    TerminalNonWinner,
}

/// Immutable receipt for one admitted or terminally rejected wait delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitActivationReceipt {
    /// Frozen receipt version.
    pub receipt_version: String,
    /// Exact external delivery proposal.
    pub activation: WaitActivation,
    /// Selected waits on which this activation won while pending.
    pub applied_wait_ids: BTreeSet<String>,
    /// Runs made ready by the winning target subset.
    pub ready_run_ids: BTreeSet<String>,
}

impl WaitActivationReceipt {
    /// Verify the receipt and embedded activation.
    ///
    /// # Errors
    ///
    /// Returns an error when the version or activation is invalid, applied
    /// targets are not a subset, or a non-winner receipt readies a Run.
    pub fn verify(&self) -> DurableProtocolResult<()> {
        if self.receipt_version != WAIT_ACTIVATION_RECEIPT_VERSION {
            return Err(DurableProtocolError::Validation(format!(
                "unsupported wait activation receipt version {:?}",
                self.receipt_version
            )));
        }
        self.activation.verify()?;
        if !self.applied_wait_ids.is_subset(&self.activation.wait_ids) {
            return Err(DurableProtocolError::Validation(
                "wait activation receipt applied targets are not selected targets".to_owned(),
            ));
        }
        if self.applied_wait_ids.is_empty() && !self.ready_run_ids.is_empty() {
            return Err(DurableProtocolError::Validation(
                "terminal non-winner activation receipt cannot ready a Run".to_owned(),
            ));
        }
        if self.ready_run_ids.len() > self.applied_wait_ids.len() {
            return Err(DurableProtocolError::Validation(
                "wait activation receipt cannot ready more Runs than applied waits".to_owned(),
            ));
        }
        for run_id in &self.ready_run_ids {
            validate_identity("wait activation ready Run", run_id)?;
        }
        Ok(())
    }

    /// Derive the aggregate transport disposition.
    pub fn disposition(&self) -> WaitActivationDisposition {
        if self.applied_wait_ids.is_empty() {
            WaitActivationDisposition::TerminalNonWinner
        } else {
            WaitActivationDisposition::Applied
        }
    }
}

/// Provider-neutral source identity for one external wait activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitActivationSource {
    /// One durable signal delivery under a correlation key.
    Signal {
        /// Correlation key declared by the waiting Plan.
        key: String,
    },
    /// One logical timer firing.
    Timer {
        /// Stable timer identity declared by the waiting Plan.
        timer_id: String,
    },
}

impl WaitActivationSource {
    /// Validate the closed source kind and identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the source identity is malformed.
    pub fn verify(&self) -> DurableProtocolResult<()> {
        match self {
            Self::Signal { key } => validate_identity("wait activation signal", key),
            Self::Timer { timer_id } => validate_identity("wait activation timer", timer_id),
        }
    }

    /// Validate signal consume-once and timer target cardinality.
    ///
    /// # Errors
    ///
    /// Returns an error when one signal would consume multiple consume-once
    /// waits or one timer does not select exactly one target.
    pub fn validate_target_cardinality(
        &self,
        target_count: usize,
        consume_once_targets: usize,
    ) -> DurableProtocolResult<()> {
        if target_count == 0 || target_count > MAX_WAIT_DELIVERY_TARGETS {
            return Err(DurableProtocolError::Validation(format!(
                "wait activation source must target 1..={MAX_WAIT_DELIVERY_TARGETS} waits"
            )));
        }
        match self {
            Self::Signal { .. } if consume_once_targets <= 1 => Ok(()),
            Self::Signal { .. } => Err(DurableProtocolError::Validation(
                "one signal activation cannot consume more than one consume-once wait".to_owned(),
            )),
            Self::Timer { .. } if target_count == 1 => Ok(()),
            Self::Timer { .. } => Err(DurableProtocolError::Validation(
                "one timer activation must target exactly one wait".to_owned(),
            )),
        }
    }
}

/// Derive the canonical Continuation identity for one Run.
///
/// # Errors
///
/// Returns an error when the Run identity is malformed or canonical identity
/// derivation fails.
pub fn continuation_id(run_id: &str) -> DurableProtocolResult<String> {
    validate_identity("Continuation Run", run_id)?;
    content_id(CONTINUATION_ID_DOMAIN, &ContinuationIdPreimage { run_id }).map_err(Into::into)
}

fn validate_identity(kind: &str, value: &str) -> DurableProtocolResult<()> {
    if value.is_empty() || value.chars().count() > 512 || value.chars().any(char::is_control) {
        return Err(DurableProtocolError::Validation(format!(
            "{kind} must contain 1..=512 non-control Unicode scalar values"
        )));
    }
    Ok(())
}

fn validate_sha256(kind: &str, value: &str) -> DurableProtocolResult<()> {
    let valid = value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    });
    if !valid {
        return Err(DurableProtocolError::Validation(format!(
            "{kind} must be a lowercase sha256 identity"
        )));
    }
    Ok(())
}

fn validate_indices(kind: &str, values: &[usize]) -> DurableProtocolResult<()> {
    for value in values {
        validate_exact_usize(kind, *value)?;
    }
    Ok(())
}

fn validate_count(kind: &str, value: usize, maximum: usize) -> DurableProtocolResult<()> {
    if value > maximum {
        return Err(DurableProtocolError::Validation(format!(
            "{kind} exceeds the fixed maximum of {maximum}"
        )));
    }
    Ok(())
}

fn account_items(total: &mut usize, added: usize, kind: &str) -> DurableProtocolResult<()> {
    *total = total.checked_add(added).ok_or_else(|| {
        DurableProtocolError::Validation(format!("{kind} overflows platform accounting"))
    })?;
    validate_count(kind, *total, MAX_CONTINUATION_AGGREGATE_ITEMS)
}

fn account_identity_scalars(total: &mut usize, value: &str) -> DurableProtocolResult<()> {
    let scalars = value.chars().count();
    *total = total.checked_add(scalars).ok_or_else(|| {
        DurableProtocolError::Validation(
            "Continuation identity scalar accounting overflowed".to_owned(),
        )
    })?;
    validate_count(
        "Continuation identity scalars",
        *total,
        MAX_CONTINUATION_IDENTITY_SCALARS,
    )
}

fn account_artifact_ref_identity_scalars(
    total: &mut usize,
    reference: &ArtifactRef,
) -> DurableProtocolResult<()> {
    for identity in [
        &reference.identity_version,
        &reference.artifact_id,
        &reference.kind,
    ] {
        account_identity_scalars(total, identity)?;
    }
    Ok(())
}

fn account_clock_ref_identity_scalars(
    total: &mut usize,
    reference: &ClockObservationRef,
) -> DurableProtocolResult<()> {
    for identity in [
        &reference.clock_version,
        &reference.observation_id,
        &reference.source_id,
        &reference.source_generation,
        &reference.scope,
    ] {
        account_identity_scalars(total, identity)?;
    }
    Ok(())
}

fn account_execution_claim_identity_scalars(
    total: &mut usize,
    claim: &ContinuationExecutionClaim,
) -> DurableProtocolResult<()> {
    for identity in [
        &claim.claim_version,
        &claim.run_id,
        &claim.continuation_id,
        &claim.owner,
        &claim.continuation_attempt_id,
        &claim.plan_id,
    ] {
        account_identity_scalars(total, identity)?;
    }
    account_artifact_ref_identity_scalars(total, &claim.execution_binding_ref)?;
    account_clock_ref_identity_scalars(total, &claim.clock_observation_ref)
}

struct BoundedWriter {
    remaining: usize,
    exceeded: bool,
}

impl BoundedWriter {
    const fn new(maximum: usize) -> Self {
        Self {
            remaining: maximum,
            exceeded: false,
        }
    }
}

impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "protocol byte bound exceeded",
            ));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn verify_compact_wire_bound<T: Serialize>(
    kind: &str,
    value: &T,
    maximum: usize,
) -> DurableProtocolResult<()> {
    let mut writer = BoundedWriter::new(maximum);
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        if writer.exceeded {
            return Err(DurableProtocolError::Validation(format!(
                "{kind} exceeds {maximum} compact wire JSON bytes"
            )));
        }
        return Err(DurableProtocolError::Encoding(error.to_string()));
    }
    Ok(())
}

fn validate_exact_usize(kind: &str, value: usize) -> DurableProtocolResult<()> {
    if u64::try_from(value).map_or(true, |value| value > MAX_EXACT_INTEGER) {
        return Err(DurableProtocolError::Validation(format!(
            "{kind} exceeds the exact cross-language integer range"
        )));
    }
    Ok(())
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
