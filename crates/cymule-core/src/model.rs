use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ir::{DispatchPolicy, EffectProfile, MutationKind, ReconciliationMode};
use crate::sha256_bytes;
use crate::{CoreError, Result, canonical_digest, content_id};

/// Semantic specification version.
pub const SEMANTIC_VERSION: &str = "cymule.semantic/6";
/// Canonical event version.
pub const EVENT_VERSION: &str = "cymule.event/8";
/// Public command version.
pub const COMMAND_VERSION: &str = "cymule.command/6";
/// Canonical Artifact reference identity version.
pub const ARTIFACT_IDENTITY_VERSION: &str = "cymule.artifact/2";
/// Effect-contract selector included in every structural intent identity.
pub const EFFECT_SCHEMA_VERSION: &str = "cymule.effect-schema/1";
/// Canonical Artifact kind for evaluated Effect input.
pub const EFFECT_ARGS_ARTIFACT_KIND: &str = "cymule.effect-args/1";
/// Canonical Artifact kind for a declared terminal failure detail.
pub const DECLARED_FAILURE_ARTIFACT_KIND: &str = "cymule.declared-failure/1";
/// Canonical Artifact kind for ordinary Component output.
pub const COMPONENT_OUTPUT_ARTIFACT_KIND: &str = "cymule.component-output/1";
const EFFECT_INTENT_ID_DOMAIN: &str = "cymule.effect-intent/2";
const EFFECT_OBLIGATION_ID_DOMAIN: &str = "cymule.effect-obligation/1";
const INVOCATION_ID_DOMAIN: &str = "cymule.invocation/2";
const SCOPE_ID_DOMAIN: &str = "cymule.scope/2";
/// Artifact kind of the exact executable binding pinned by Runs and
/// provider-facing occurrences.
pub const EXECUTION_BINDING_ARTIFACT_KIND: &str = "cymule.execution-binding/2";
/// Artifact kind of the immutable input admitted with a new Run.
pub const RUN_INPUT_ARTIFACT_KIND: &str = "cymule.input/1";
/// Largest non-negative integer represented exactly by every supported SDK.
pub const MAX_EXACT_INTEGER: u64 = 9_007_199_254_740_991;
/// Stable root scope identifier within every Run.
pub const ROOT_SCOPE_ID: &str = "scope:root";
/// Maximum ASCII bytes in a validated Artifact kind.
pub const MAX_ARTIFACT_KIND_BYTES: usize = 255;
/// Maximum raw bytes in any Core Artifact.
///
/// Artifact bytes use strict padded Base64 on JSON wires, so the worst-case
/// canonical `ArtifactRecord` remains below the durable 12 MiB leaf bound.
pub const MAX_ARTIFACT_BYTES: usize = 8 * 1024 * 1024;
/// Exact maximum padded-Base64 code units for [`MAX_ARTIFACT_BYTES`].
pub const MAX_ARTIFACT_BASE64_BYTES: usize = MAX_ARTIFACT_BYTES.div_ceil(3) * 4;
/// Maximum canonical JSON bytes of one persisted [`ArtifactRecord`].
pub const MAX_ARTIFACT_RECORD_CANONICAL_BYTES: usize = 12 * 1024 * 1024;

/// Immutable artifact reference.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    /// Artifact identity algorithm and wire version.
    pub identity_version: String,
    /// Content-addressed identity.
    pub artifact_id: String,
    /// Stable artifact type.
    pub kind: String,
}

/// Immutable artifact bytes held by the embedded store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRecord {
    /// Reference derived from type and bytes.
    pub reference: ArtifactRef,
    /// Immutable bytes.
    #[serde(with = "canonical_base64_bytes")]
    pub bytes: Vec<u8>,
}

impl ArtifactRecord {
    /// Decode one exact canonical Artifact record with pre-allocation bounds.
    ///
    /// # Errors
    ///
    /// Returns an error before Base64 allocation when the record or encoded
    /// bytes exceed their bounds, when the Base64 string uses escapes or a
    /// noncanonical representation, or when the decoded record/identity is not
    /// exact canonical authority.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_ARTIFACT_RECORD_CANONICAL_BYTES {
            return Err(CoreError::Encoding(format!(
                "Artifact record has {} canonical bytes; maximum is {MAX_ARTIFACT_RECORD_CANONICAL_BYTES}",
                bytes.len()
            )));
        }
        validate_raw_artifact_base64_field(bytes)?;
        let record: Self = crate::decode_json(bytes)?;
        if crate::canonical_bytes(&record)? != bytes {
            return Err(CoreError::Encoding(
                "Artifact record is not exact canonical JSON".to_owned(),
            ));
        }
        record.validate()?;
        Ok(record)
    }

    /// Verify the bounded bytes and exact typed content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes exceed [`MAX_ARTIFACT_BYTES`] or the
    /// retained reference does not exactly identify the kind and raw bytes.
    pub fn validate(&self) -> Result<()> {
        self.reference.validate()?;
        let expected = artifact_ref(self.reference.kind.clone(), &self.bytes)?;
        if expected != self.reference {
            return Err(CoreError::IdentityMismatch(format!(
                "Artifact {} does not match its bytes",
                self.reference.artifact_id
            )));
        }
        Ok(())
    }
}

fn validate_raw_artifact_base64_field(bytes: &[u8]) -> Result<()> {
    let mut index = 0_usize;
    let mut found = false;
    while index < bytes.len() {
        if bytes[index] != b'"' {
            index += 1;
            continue;
        }
        let key_start = index + 1;
        let (key_end, key_escaped) = scan_raw_string(bytes, index)?;
        let mut cursor = key_end;
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= bytes.len() || bytes[cursor] != b':' {
            index = key_end;
            continue;
        }
        if key_escaped {
            return Err(CoreError::Encoding(
                "Artifact record member names must not use JSON escapes".to_owned(),
            ));
        }
        cursor += 1;
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if &bytes[key_start..key_end - 1] != b"bytes" {
            index = key_end;
            continue;
        }
        if found || cursor >= bytes.len() || bytes[cursor] != b'"' {
            return Err(CoreError::Encoding(
                "Artifact record must contain one Base64 bytes string".to_owned(),
            ));
        }
        let value_start = cursor + 1;
        let (value_end, value_escaped) = scan_raw_string(bytes, cursor)?;
        let encoded_len = value_end - value_start - 1;
        if value_escaped || encoded_len > MAX_ARTIFACT_BASE64_BYTES {
            return Err(CoreError::Encoding(format!(
                "Artifact bytes must be unescaped padded Base64 with at most {MAX_ARTIFACT_BASE64_BYTES} code units"
            )));
        }
        found = true;
        index = value_end;
    }
    if !found {
        return Err(CoreError::Encoding(
            "Artifact record has no bytes field".to_owned(),
        ));
    }
    Ok(())
}

fn scan_raw_string(bytes: &[u8], start: usize) -> Result<(usize, bool)> {
    let mut index = start
        .checked_add(1)
        .ok_or_else(|| CoreError::Encoding("JSON string offset overflowed".to_owned()))?;
    let mut escaped = false;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => return Ok((index + 1, escaped)),
            b'\\' => {
                escaped = true;
                index = index.checked_add(2).ok_or_else(|| {
                    CoreError::Encoding("JSON escape offset overflowed".to_owned())
                })?;
            }
            _ => index += 1,
        }
    }
    Err(CoreError::Encoding(
        "JSON string is not terminated".to_owned(),
    ))
}

mod canonical_base64_bytes {
    use std::fmt;

    use base64::Engine as _;
    use serde::de::Visitor;
    use serde::{Deserializer, Serializer};

    struct CanonicalBase64Visitor;

    impl Visitor<'_> for CanonicalBase64Visitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("canonical padded Base64 Artifact bytes")
        }

        fn visit_borrowed_str<E>(self, value: &'_ str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            decode(value).map_err(E::custom)
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            decode(value).map_err(E::custom)
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            decode(&value).map_err(E::custom)
        }
    }

    fn decode(encoded: &str) -> Result<Vec<u8>, &'static str> {
        if encoded.len() > super::MAX_ARTIFACT_BASE64_BYTES || !encoded.len().is_multiple_of(4) {
            return Err("Artifact Base64 exceeds the encoded bound or has invalid padding length");
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| "Artifact bytes are not padded Base64")?;
        if bytes.len() > super::MAX_ARTIFACT_BYTES
            || base64::engine::general_purpose::STANDARD.encode(&bytes) != encoded
        {
            return Err("Artifact bytes must use canonical padded Base64");
        }
        Ok(bytes)
    }

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(CanonicalBase64Visitor)
    }
}

/// Derive the canonical content reference for immutable typed bytes.
///
/// This is the sole implementation of the `cymule.artifact/2` identity
/// preimage. Stores and typed-contract layers must call it rather than duplicate
/// the domain separator or framing.
///
/// # Errors
///
/// Returns a validation error when `kind` is invalid or either preimage length
/// cannot be represented by the versioned identity framing.
pub fn artifact_ref(kind: impl Into<String>, bytes: &[u8]) -> Result<ArtifactRef> {
    let kind = kind.into();
    validate_artifact_kind(&kind)?;
    if bytes.len() > MAX_ARTIFACT_BYTES {
        return Err(CoreError::Validation(format!(
            "Artifact has {} raw bytes; maximum is {MAX_ARTIFACT_BYTES}",
            bytes.len()
        )));
    }
    let kind_len = u32::try_from(kind.len())
        .map_err(|_| CoreError::Validation("Artifact kind length exceeds u32".to_owned()))?;
    let bytes_len = u64::try_from(bytes.len())
        .map_err(|_| CoreError::Validation("Artifact byte length exceeds u64".to_owned()))?;
    let mut preimage = Vec::with_capacity(kind.len() + bytes.len() + 36);
    preimage.extend_from_slice(ARTIFACT_IDENTITY_VERSION.as_bytes());
    preimage.extend_from_slice(&kind_len.to_be_bytes());
    preimage.extend_from_slice(kind.as_bytes());
    preimage.extend_from_slice(&bytes_len.to_be_bytes());
    preimage.extend_from_slice(bytes);
    Ok(ArtifactRef {
        identity_version: ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: format!("sha256:{}", sha256_bytes(&preimage)),
        kind,
    })
}

impl ArtifactRef {
    /// Validate the closed Artifact identity version, digest, and kind shape.
    ///
    /// # Errors
    ///
    /// Returns a validation error when any identity component violates the
    /// closed Artifact-reference contract.
    pub fn validate(&self) -> Result<()> {
        if self.identity_version != ARTIFACT_IDENTITY_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported Artifact identity version {:?}",
                self.identity_version
            )));
        }
        validate_artifact_kind(&self.kind)?;
        let Some(digest) = self.artifact_id.strip_prefix("sha256:") else {
            return Err(CoreError::Validation(
                "Artifact ID must use lowercase sha256".to_owned(),
            ));
        };
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CoreError::Validation(
                "Artifact ID must contain 64 lowercase hexadecimal digits".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Validate the closed lowercase versioned-path syntax used by Artifact kinds.
///
/// # Errors
///
/// Returns a validation error unless `kind` is one bounded ASCII lowercase
/// versioned path.
pub fn validate_artifact_kind(kind: &str) -> Result<()> {
    if kind.is_empty()
        || kind.len() > MAX_ARTIFACT_KIND_BYTES
        || !kind.is_ascii()
        || !kind.contains('/')
        || kind.split('/').any(|segment| {
            segment.is_empty()
                || !segment.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'_' | b'-' | b'+')
                })
        })
    {
        return Err(CoreError::Validation(
            "Artifact kind must be 1..=255 bytes of lowercase versioned path segments".to_owned(),
        ));
    }
    Ok(())
}

fn deserialize_required_nullable<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

/// A preconditioned, idempotent command proposal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandEnvelope {
    /// Command protocol version.
    pub command_version: String,
    /// Caller-generated idempotency identity.
    pub command_id: String,
    /// Actor provenance supplied by the embedding. Core validates its shape;
    /// authentication and authorization belong to the embedding boundary.
    pub actor: String,
    /// Target Run identity.
    pub run_id: String,
    /// Token from the caller's current Run view.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub expected_precondition: Option<String>,
    /// Typed semantic proposal.
    pub command: Command,
}

/// Exact initial execution Attempt admitted atomically with a new Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialAttemptSpec {
    /// Content-addressed Attempt identity.
    pub attempt_id: String,
    /// Content-addressed initial Continuation identity.
    pub continuation_id: String,
    /// Exact occurrence binding; `StartRun` requires the execution binding ID.
    pub occurrence_binding: String,
    /// Initial Continuation epoch; exactly zero.
    pub continuation_epoch: u64,
    /// First durable execution fence; exactly one.
    pub execution_fence: u64,
}

impl InitialAttemptSpec {
    /// Verify the closed first-Attempt semantics against its execution binding.
    ///
    /// # Errors
    ///
    /// Returns an error unless all identities are content-addressed, the
    /// occurrence binding equals `binding_context`, the epoch is zero, and the
    /// first fence is one.
    pub fn verify(&self, binding_context: &str) -> Result<()> {
        crate::validate_content_id("initial Attempt", &self.attempt_id)?;
        crate::validate_content_id("initial Continuation", &self.continuation_id)?;
        crate::validate_content_id("initial occurrence binding", &self.occurrence_binding)?;
        if self.occurrence_binding != binding_context
            || self.continuation_epoch != 0
            || self.execution_fence != 1
        {
            return Err(CoreError::Validation(
                "StartRun initial Attempt must bind the execution context at epoch 0 and fence 1"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// Commands admitted by the semantic kernel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    /// Start a new Run under an immutable plan and future-default binding context.
    StartRun {
        /// Sealed plan ID.
        plan_id: String,
        /// Default realization context for future occurrences.
        binding_context: String,
        /// Exact immutable initial input admitted with the Run.
        input: ArtifactRef,
        /// Digest of the complete bounded immutable-material admission.
        material_digest: String,
        /// First Attempt admitted in the same command Event batch.
        initial_attempt: InitialAttemptSpec,
    },
    /// Begin a fenced Attempt.
    BeginAttempt {
        /// Attempt identity.
        attempt_id: String,
        /// Continuation identity.
        continuation_id: String,
        /// Occurrence-level immutable binding.
        occurrence_binding: String,
        /// Expected continuation epoch.
        continuation_epoch: u64,
        /// Durable execution-claim fence authorizing this Attempt.
        execution_fence: u64,
    },
    /// Yield the active Attempt at a safe point.
    YieldAttempt {
        /// Attempt identity.
        attempt_id: String,
        /// Expected continuation epoch.
        continuation_epoch: u64,
        /// Durable execution-claim fence authorizing this yield.
        execution_fence: u64,
    },
    /// Advance the Run epoch and fence prior attempts.
    AdvanceEpoch,
    /// Open a nested state/evidence scope.
    OpenScope {
        /// New scope identity.
        scope_id: String,
        /// Existing parent scope.
        parent_scope: String,
        /// Exact dynamic invocation identity opening the scope.
        invocation_id: String,
        /// Entry-rooted invocation path.
        invocation_path: Vec<InvocationPathSegment>,
        /// Definition containing the scope site.
        definition_id: String,
        /// Region containing the scope site.
        region_path: Vec<usize>,
        /// Stable scope operation site.
        site_id: String,
    },
    /// Propose a structurally identified external effect.
    ProposeEffect {
        /// Owning open scope.
        scope_id: String,
        /// Invocation identity.
        invocation_id: String,
        /// Entry-rooted invocation path proving the dynamic invocation.
        invocation_path: Vec<InvocationPathSegment>,
        /// Definition containing the effect site.
        definition_id: String,
        /// Exact lexical Region containing the effect site.
        region_path: Vec<usize>,
        /// Stable IR effect site.
        site_id: String,
        /// Intentional occurrence key.
        occurrence: String,
        /// Abstract effect operation.
        operation: String,
        /// Canonical argument artifact.
        args: ArtifactRef,
        /// Exact executable binding Artifact selected at admission.
        execution_binding: ArtifactRef,
        /// Immutable occurrence binding.
        occurrence_binding: String,
    },
    /// Advance one axis of an existing effect.
    TransitionEffect {
        /// Structural intent identity.
        intent_id: String,
        /// Legal next transition.
        transition: EffectTransition,
    },
    /// Commit internal scope state and transfer unresolved effect obligations.
    CommitScope {
        /// Scope identity.
        scope_id: String,
    },
    /// Abort a scope before release.
    AbortScope {
        /// Scope identity.
        scope_id: String,
    },
    /// Change the realization default for future occurrences.
    UpdateBinding {
        /// New immutable Binding Context reference.
        binding_context: String,
    },
    /// Replace a quiescent Run's exact Plan and execution binding under a
    /// higher-profile migration proof. The owning durable CAS validates that
    /// proof, state compatibility, and Continuation replacement atomically.
    MigrateRun {
        /// Exact source Plan expected at the safe point.
        from_plan: String,
        /// Exact target Plan admitted for resumed execution.
        to_plan: String,
        /// Source `ExecutionBinding` Artifact identity.
        from_binding: String,
        /// Target `ExecutionBinding` Artifact identity.
        to_binding: String,
        /// Content-addressed higher-profile safe-point proof identity.
        safe_point_id: String,
        /// Exact target Continuation epoch committed by the owning CAS.
        target_epoch: u64,
        /// Canonical digest of the complete target Continuation/frame stack.
        target_continuation_digest: String,
    },
    /// Append a Machine-wide immutable application fact.
    ///
    /// Facts are a small, general causal primitive used by conformance tests
    /// and applications; they are not test-only state.
    RecordFact {
        /// Stable logical fact key.
        key: String,
        /// Immutable value digest or reference.
        value: String,
    },
    /// Finish a Run after every external-world Effect has settled.
    CompleteRun {
        /// Optional typed Result artifact.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        result: Option<ArtifactRef>,
    },
    /// Fail a Run with one typed terminal reason.
    FailRun {
        /// Typed failure classification and immutable detail.
        failure: RunFailure,
    },
    /// Cancel a Run with one immutable semantic reason.
    CancelRun {
        /// Content-addressed cancellation reason.
        reason: ArtifactRef,
    },
}

/// Legal effect transitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum EffectTransition {
    /// Preparation completed.
    Prepare,
    /// Governing policy authorized release.
    AuthorizeRelease,
    /// External dispatch began.
    StartDispatch,
    /// Record the best authoritative world observation while execution is active.
    Observe(WorldOutcome),
    /// Reconcile an earlier unknown outcome, including after failure or cancellation.
    Reconcile(ReconciliationResolution),
    /// The exact admitted execution binding cannot currently be realized.
    MarkUnavailable,
}

/// A canonical event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Content-addressed event identity.
    pub event_id: String,
    /// Event schema version.
    pub event_version: String,
    /// Admitted command identity.
    pub command_id: String,
    /// Canonical command semantics digest.
    pub command_hash: String,
    /// Run identity.
    pub run_id: String,
    /// Explicit causal parents.
    pub parents: Vec<String>,
    /// Logical point/predicate reads.
    pub reads: BTreeSet<String>,
    /// Logical writes.
    pub writes: BTreeSet<String>,
    /// Optional non-monotone coordination domain.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub coordination_key: Option<String>,
    /// Trusted semantic transition.
    pub payload: EventPayload,
}

#[derive(Serialize)]
struct EventPreimage<'a> {
    event_version: &'a str,
    command_id: &'a str,
    command_hash: &'a str,
    run_id: &'a str,
    parents: &'a [String],
    reads: &'a BTreeSet<String>,
    writes: &'a BTreeSet<String>,
    coordination_key: &'a Option<String>,
    payload: &'a EventPayload,
}

/// Untrusted semantic content from which an [`Event`] is content-addressed.
///
/// Constructing an Event does not admit it or grant mutation authority. Only a
/// typed command transition inside [`crate::Machine`] may publish the Event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventContent {
    /// Admitted command identity.
    pub command_id: String,
    /// Canonical command semantics digest.
    pub command_hash: String,
    /// Run identity.
    pub run_id: String,
    /// Explicit causal parents.
    pub parents: Vec<String>,
    /// Logical point/predicate reads.
    pub reads: BTreeSet<String>,
    /// Logical writes.
    pub writes: BTreeSet<String>,
    /// Optional non-monotone coordination domain.
    pub coordination_key: Option<String>,
    /// Semantic transition content.
    pub payload: EventPayload,
}

impl Event {
    /// Construct and content-address untrusted Event content.
    ///
    /// This operation validates and derives identity only. It does not admit
    /// the Event or mutate a Machine.
    ///
    /// # Errors
    ///
    /// Returns an error when the command, Run, parent, payload, or derived
    /// identity content violates the canonical Event contract.
    pub fn new(content: EventContent) -> Result<Self> {
        let EventContent {
            command_id,
            command_hash,
            run_id,
            mut parents,
            reads,
            writes,
            coordination_key,
            payload,
        } = content;
        validate_event_header(&command_id, &command_hash, &run_id)?;
        parents.sort();
        parents.dedup();
        for parent in &parents {
            crate::validate_content_id("Event parent", parent)?;
        }
        let preimage = EventPreimage {
            event_version: EVENT_VERSION,
            command_id: &command_id,
            command_hash: &command_hash,
            run_id: &run_id,
            parents: &parents,
            reads: &reads,
            writes: &writes,
            coordination_key: &coordination_key,
            payload: &payload,
        };
        let event_id = content_id(EVENT_VERSION, &preimage)?;
        Ok(Self {
            event_id,
            event_version: EVENT_VERSION.to_owned(),
            command_id,
            command_hash,
            run_id,
            parents,
            reads,
            writes,
            coordination_key,
            payload,
        })
    }

    /// Verify event schema and content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the Event has invalid typed payload authority,
    /// malformed causal metadata, or a mismatched content identity.
    pub fn verify(&self) -> Result<()> {
        if self.event_version != EVENT_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported event version {:?}",
                self.event_version
            )));
        }
        validate_event_header(&self.command_id, &self.command_hash, &self.run_id)?;
        if self
            .parents
            .windows(2)
            .any(|pair| pair[0].as_str() >= pair[1].as_str())
        {
            return Err(CoreError::Validation(
                "event parents must be strictly sorted and duplicate-free".to_owned(),
            ));
        }
        for parent in &self.parents {
            crate::validate_content_id("Event parent", parent)?;
        }
        match &self.payload {
            EventPayload::RunStarted { input, .. } => {
                input.validate()?;
                if input.kind != RUN_INPUT_ARTIFACT_KIND {
                    return Err(CoreError::Validation(
                        "Run input has the wrong Artifact kind".to_owned(),
                    ));
                }
            }
            EventPayload::EffectProposed {
                args,
                execution_binding,
                ..
            } => {
                validate_effect_args_reference(args)?;
                execution_binding.validate()?;
                if execution_binding.kind != EXECUTION_BINDING_ARTIFACT_KIND {
                    return Err(CoreError::Validation(
                        "effect execution binding has the wrong Artifact kind".to_owned(),
                    ));
                }
            }
            EventPayload::ScopeCommitted {
                obligation_count,
                obligation_commitment,
                ..
            } => {
                if *obligation_count > MAX_EXACT_INTEGER {
                    return Err(CoreError::Validation(
                        "scope obligation count exceeds the exact integer range".to_owned(),
                    ));
                }
                crate::validate_content_id("scope obligation commitment", obligation_commitment)?;
            }
            EventPayload::RunCompleted {
                result: Some(result),
            } => result.validate()?,
            EventPayload::RunFailed { failure, .. } => failure.verify()?,
            EventPayload::RunCancelled { reason, .. } => reason.validate()?,
            EventPayload::FactRecorded { key, value } => {
                crate::validate_identity("fact key", key)?;
                crate::validate_content_id("fact value", value)?;
            }
            _ => {}
        }
        let preimage = EventPreimage {
            event_version: &self.event_version,
            command_id: &self.command_id,
            command_hash: &self.command_hash,
            run_id: &self.run_id,
            parents: &self.parents,
            reads: &self.reads,
            writes: &self.writes,
            coordination_key: &self.coordination_key,
            payload: &self.payload,
        };
        let expected = content_id(EVENT_VERSION, &preimage)?;
        if expected != self.event_id {
            return Err(CoreError::IdentityMismatch(format!(
                "event ID {} does not match {expected}",
                self.event_id
            )));
        }
        Ok(())
    }
}

fn validate_event_header(command_id: &str, command_hash: &str, run_id: &str) -> Result<()> {
    crate::validate_identity("Event command", command_id)?;
    crate::validate_identity("Event Run", run_id)?;
    if command_hash.len() != 64
        || !command_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CoreError::Validation(
            "Event command hash must be an exact lowercase SHA-256 digest".to_owned(),
        ));
    }
    Ok(())
}

/// Canonical transition payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventPayload {
    /// A Run became canonical.
    RunStarted {
        /// Plan identity.
        plan_id: String,
        /// Immutable entry definition identity.
        entry_definition: String,
        /// Future-default realization context.
        binding_context: String,
        /// Exact immutable initial input admitted with the Run.
        input: ArtifactRef,
    },
    /// An Attempt received a fenced lease.
    AttemptStarted {
        /// Attempt identity.
        attempt_id: String,
        /// Continuation identity.
        continuation_id: String,
        /// Pinned occurrence binding.
        occurrence_binding: String,
        /// Pinned continuation epoch.
        continuation_epoch: u64,
        /// Durable execution-claim fence.
        execution_fence: u64,
    },
    /// An Attempt yielded at a safe point.
    AttemptYielded {
        /// Attempt identity.
        attempt_id: String,
        /// Pinned continuation epoch.
        continuation_epoch: u64,
        /// Durable execution-claim fence.
        execution_fence: u64,
    },
    /// The Run epoch advanced.
    EpochAdvanced {
        /// New epoch.
        epoch: u64,
    },
    /// A nested scope opened.
    ScopeOpened {
        /// Scope identity.
        scope_id: String,
        /// Parent scope identity.
        parent_scope: String,
        /// Exact dynamic invocation identity that opened the scope.
        invocation_id: String,
        /// Entry-rooted invocation path.
        invocation_path: Vec<InvocationPathSegment>,
        /// Definition containing the scope site.
        definition_id: String,
        /// Region path of the opened scope body.
        region_path: Vec<usize>,
        /// Stable scope operation site.
        site_id: String,
    },
    /// An effect was admitted with an immutable occurrence binding.
    EffectProposed {
        /// Structural intent identity.
        intent_id: String,
        /// Immutable Plan which defined this effect occurrence.
        origin_plan_id: String,
        /// Owning scope.
        scope_id: String,
        /// Exact dynamic invocation identity.
        invocation_id: String,
        /// Entry-rooted invocation path proving the dynamic invocation.
        invocation_path: Vec<InvocationPathSegment>,
        /// Definition containing the effect site.
        definition_id: String,
        /// Exact lexical Region containing the effect site.
        region_path: Vec<usize>,
        /// Stable reachable IR effect site.
        site_id: String,
        /// Intentional occurrence key declared at the site.
        occurrence: String,
        /// Effect identity schema version.
        effect_schema_version: String,
        /// Abstract operation ID.
        operation: String,
        /// Complete Plan-declared safety and recovery profile.
        profile: EffectProfile,
        /// Canonical argument artifact.
        args: Box<ArtifactRef>,
        /// Exact executable binding Artifact selected for this occurrence.
        execution_binding: Box<ArtifactRef>,
        /// Pinned occurrence binding.
        occurrence_binding: String,
    },
    /// One effect state axis advanced.
    EffectTransitioned {
        /// Structural intent identity.
        intent_id: String,
        /// Transition.
        transition: EffectTransition,
    },
    /// Internal scope state committed and obligations transferred.
    ScopeCommitted {
        /// Scope identity.
        scope_id: String,
        /// Number of deterministically derived obligations.
        obligation_count: u64,
        /// Core-owned proposal-order commitment over every derived obligation.
        obligation_commitment: String,
    },
    /// A scope aborted before effect release.
    ScopeAborted {
        /// Scope identity.
        scope_id: String,
    },
    /// Future occurrences use a new default binding context.
    BindingUpdated {
        /// Previous context.
        previous: String,
        /// New context.
        current: String,
    },
    /// A verified higher profile migrated a quiescent Run to a new Plan and
    /// execution binding before advancing its Attempt epoch.
    RunMigrated {
        /// Exact source Plan.
        from_plan: String,
        /// Exact target Plan.
        to_plan: String,
        /// Source `ExecutionBinding` Artifact identity.
        from_binding: String,
        /// Target `ExecutionBinding` Artifact identity.
        to_binding: String,
        /// Content-addressed migration-safe-point proof identity.
        safe_point_id: String,
        /// Exact target Continuation epoch committed by the owning CAS.
        target_epoch: u64,
        /// Canonical digest of the complete target Continuation/frame stack.
        target_continuation_digest: String,
    },
    /// An append-only fact was recorded.
    FactRecorded {
        /// Logical fact key.
        key: String,
        /// Immutable value.
        value: String,
    },
    /// A Run entered a terminal completed state.
    RunCompleted {
        /// Optional Result artifact.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        result: Option<ArtifactRef>,
    },
    /// A Run entered a terminal failed execution state.
    RunFailed {
        /// Typed failure classification and immutable detail.
        failure: RunFailure,
        /// New execution fence invalidating every prior Attempt.
        epoch: u64,
    },
    /// A Run entered a terminal cancelled execution state.
    RunCancelled {
        /// Content-addressed cancellation reason.
        reason: ArtifactRef,
        /// New execution fence invalidating every prior Attempt.
        epoch: u64,
    },
}

/// Public command outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandReceiptStatus {
    /// A canonical event was admitted.
    Applied,
    /// The caller's precondition was stale.
    Conflict,
}

/// Durable command receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandReceipt {
    /// Command identity.
    pub command_id: String,
    /// Stable status.
    pub status: CommandReceiptStatus,
    /// Complete admitted Event batch in canonical order.
    pub event_ids: Vec<String>,
    /// Stable structured error code for a conflict.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub error_code: Option<String>,
    /// Human-readable explanation.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub message: Option<String>,
    /// Caller-provided token.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub observed_precondition: Option<String>,
    /// Current token after application or conflict.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub current_precondition: Option<String>,
}

/// Rebuildable full projection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    /// Run projections.
    pub runs: BTreeMap<String, RunProjection>,
    /// Append-only facts used by explainability and conformance.
    pub facts: BTreeMap<String, String>,
}

/// Rebuildable Run projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunProjection {
    /// Run identity.
    pub run_id: String,
    /// Initial immutable plan.
    pub initial_plan: String,
    /// Current semantic Plan after any verified safe-point migration.
    pub current_plan: String,
    /// Exact ordered Plan lineage admitted for this Run.
    ///
    /// The first entry is the initial Plan and the last entry is the current
    /// Plan. Durable historical frames may name only a Plan retained here.
    pub plan_lineage: Vec<String>,
    /// Initial realization default.
    pub initial_binding_context: String,
    /// Future-occurrence realization default.
    pub current_binding_context: String,
    /// Exact ordered execution-binding lineage admitted for this Run. The first
    /// entry is the initial binding and the last entry is the current binding.
    pub binding_lineage: Vec<String>,
    /// Fencing epoch.
    pub epoch: u64,
    /// Canonical execution status on a separate axis from world settlement.
    /// Completion requires that settlement axis to be `Settled`.
    pub execution_status: RunExecutionStatus,
    /// Settlement of all admitted external-world intents.
    pub world_settlement: WorldSettlementStatus,
    /// Scope projections.
    pub scopes: BTreeMap<String, ScopeProjection>,
    /// Effect projections.
    pub effects: BTreeMap<String, EffectProjection>,
    /// Outstanding or settled obligations.
    pub obligations: BTreeMap<String, ObligationProjection>,
    /// Attempt projections.
    pub attempts: BTreeMap<String, AttemptProjection>,
    /// Optional terminal Result.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub result: Option<ArtifactRef>,
    /// Last applied event in the embedded linear frontier.
    pub last_event: String,
    /// Rebuildable hot reducer index. It is never serialized and never semantic
    /// authority; restore reconstructs it from the canonical fields above.
    #[serde(skip)]
    pub(crate) derived: RunDerivedIndex,
}

impl PartialEq for RunProjection {
    fn eq(&self, other: &Self) -> bool {
        self.run_id == other.run_id
            && self.initial_plan == other.initial_plan
            && self.current_plan == other.current_plan
            && self.plan_lineage == other.plan_lineage
            && self.initial_binding_context == other.initial_binding_context
            && self.current_binding_context == other.current_binding_context
            && self.binding_lineage == other.binding_lineage
            && self.epoch == other.epoch
            && self.execution_status == other.execution_status
            && self.world_settlement == other.world_settlement
            && self.scopes == other.scopes
            && self.effects == other.effects
            && self.obligations == other.obligations
            && self.attempts == other.attempts
            && self.result == other.result
            && self.last_event == other.last_event
    }
}

impl Eq for RunProjection {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RunDerivedIndex {
    pub(crate) initialized: bool,
    pub(crate) active_attempt: Option<String>,
    pub(crate) governance_effects: BTreeSet<String>,
    pub(crate) unknown_effects: BTreeSet<String>,
    pub(crate) pending_effects: BTreeSet<String>,
    pub(crate) terminal_transition_effects: BTreeSet<String>,
    pub(crate) unresolved_blocking_obligations: BTreeSet<String>,
    pub(crate) obligation_by_intent: BTreeMap<String, BTreeSet<String>>,
    /// Number of directly open child scopes. A scope cannot close while a
    /// direct child is open, so this O(1)-update index is sufficient to reject
    /// every transitive open descendant without walking the ancestor chain.
    pub(crate) open_descendants: BTreeMap<String, u64>,
    pub(crate) open_scope_ids: BTreeSet<String>,
    pub(crate) open_scope_effects: BTreeMap<String, OpenScopeEffectIndex>,
    pub(crate) effect_count_by_scope: BTreeMap<String, u64>,
    pub(crate) committed_effect_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OpenScopeEffectIndex {
    pub(crate) all_intents: BTreeSet<String>,
    pub(crate) all_intent_order: Vec<String>,
    pub(crate) mutating_intents: BTreeSet<String>,
    pub(crate) mutating_intent_order: Vec<String>,
    pub(crate) abort_transition_intents: BTreeSet<String>,
    pub(crate) abort_blockers: BTreeSet<String>,
}

impl RunDerivedIndex {
    fn rebuild(run: &RunProjection) -> Result<Self> {
        let mut index = Self {
            initialized: true,
            ..Self::default()
        };
        for attempt in run.attempts.values() {
            if attempt.active {
                if index.active_attempt.is_some() {
                    return Err(CoreError::Validation(format!(
                        "Run {} has more than one active Attempt",
                        run.run_id
                    )));
                }
                index.active_attempt = Some(attempt.attempt_id.clone());
            }
        }
        for scope in run.scopes.values() {
            if scope.status != ScopeStatus::Open {
                continue;
            }
            index.open_scope_ids.insert(scope.scope_id.clone());
            index
                .open_scope_effects
                .insert(scope.scope_id.clone(), OpenScopeEffectIndex::default());
            if let Some(parent_id) = scope.parent_scope.as_deref() {
                let count = index
                    .open_descendants
                    .entry(parent_id.to_owned())
                    .or_default();
                *count = count.checked_add(1).ok_or_else(|| {
                    CoreError::Validation("open scope descendant count overflowed".to_owned())
                })?;
            }
        }
        for scope in run.scopes.values() {
            for intent_id in &scope.intent_order {
                let effect = run.effects.get(intent_id).ok_or_else(|| {
                    CoreError::Validation(format!(
                        "scope {} references missing Effect {intent_id}",
                        scope.scope_id
                    ))
                })?;
                if effect.scope_id != scope.scope_id {
                    return Err(CoreError::Validation(format!(
                        "scope {} orders Effect {intent_id} owned by {}",
                        scope.scope_id, effect.scope_id
                    )));
                }
                index.register_effect(effect, scope.status)?;
            }
        }
        for obligation in run.obligations.values() {
            if !index
                .obligation_by_intent
                .entry(obligation.intent_id.clone())
                .or_default()
                .insert(obligation.obligation_id.clone())
            {
                return Err(CoreError::Validation(format!(
                    "Run {} repeats obligation {} in its derived index",
                    run.run_id, obligation.obligation_id
                )));
            }
            if obligation.blocking && !obligation.resolved {
                index
                    .unresolved_blocking_obligations
                    .insert(obligation.obligation_id.clone());
            }
        }
        Ok(index)
    }

    fn register_effect(
        &mut self,
        effect: &EffectProjection,
        scope_status: ScopeStatus,
    ) -> Result<()> {
        let count = self
            .effect_count_by_scope
            .entry(effect.scope_id.clone())
            .or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| CoreError::Validation("scope Effect count overflowed".to_owned()))?;
        if scope_status == ScopeStatus::ClosedCommitted {
            self.committed_effect_count = self
                .committed_effect_count
                .checked_add(1)
                .filter(|count| *count <= MAX_EXACT_INTEGER)
                .ok_or_else(|| {
                    CoreError::Validation("committed Effect count overflowed".to_owned())
                })?;
        }
        if scope_status == ScopeStatus::Open {
            let scope = self
                .open_scope_effects
                .get_mut(&effect.scope_id)
                .ok_or_else(|| {
                    CoreError::Validation(format!(
                        "open Effect {} has no open-scope index",
                        effect.intent_id
                    ))
                })?;
            if !scope.all_intents.insert(effect.intent_id.clone()) {
                return Err(CoreError::Validation(format!(
                    "open scope Effect index repeats {}",
                    effect.intent_id
                )));
            }
            scope.all_intent_order.push(effect.intent_id.clone());
            if effect.profile.mutation == MutationKind::Mutating {
                if !scope.mutating_intents.insert(effect.intent_id.clone()) {
                    return Err(CoreError::Validation(format!(
                        "open scope mutating Effect index repeats {}",
                        effect.intent_id
                    )));
                }
                scope.mutating_intent_order.push(effect.intent_id.clone());
            }
        }
        self.add_effect_state(effect, scope_status)
    }

    fn add_effect_state(
        &mut self,
        effect: &EffectProjection,
        scope_status: ScopeStatus,
    ) -> Result<()> {
        if self.governance_effects.contains(&effect.intent_id)
            || self.unknown_effects.contains(&effect.intent_id)
            || self.pending_effects.contains(&effect.intent_id)
        {
            return Err(CoreError::Validation(format!(
                "Effect {} already exists in a settlement index",
                effect.intent_id
            )));
        }
        let target = match effect_settlement_class(effect) {
            EffectSettlementClass::Governance => Some(&mut self.governance_effects),
            EffectSettlementClass::Unknown => Some(&mut self.unknown_effects),
            EffectSettlementClass::Pending => Some(&mut self.pending_effects),
            EffectSettlementClass::Settled => None,
        };
        if let Some(target) = target
            && !target.insert(effect.intent_id.clone())
        {
            return Err(CoreError::Validation(format!(
                "Effect {} already exists in its settlement index",
                effect.intent_id
            )));
        }
        if needs_terminal_transition(effect) {
            self.terminal_transition_effects
                .insert(effect.intent_id.clone());
        }
        if scope_status == ScopeStatus::Open {
            let scope = self
                .open_scope_effects
                .get_mut(&effect.scope_id)
                .ok_or_else(|| {
                    CoreError::Validation(format!(
                        "open Effect {} has no open-scope index",
                        effect.intent_id
                    ))
                })?;
            if matches!(effect.phase, EffectPhase::Admitted | EffectPhase::Prepared) {
                scope
                    .abort_transition_intents
                    .insert(effect.intent_id.clone());
            }
            if effect.profile.mutation == MutationKind::Mutating
                && matches!(
                    effect.phase,
                    EffectPhase::ReleaseAuthorized | EffectPhase::DispatchStarted
                )
            {
                scope.abort_blockers.insert(effect.intent_id.clone());
            }
        }
        Ok(())
    }

    fn remove_effect_state(
        &mut self,
        effect: &EffectProjection,
        scope_status: ScopeStatus,
    ) -> Result<()> {
        let source = match effect_settlement_class(effect) {
            EffectSettlementClass::Governance => Some(&mut self.governance_effects),
            EffectSettlementClass::Unknown => Some(&mut self.unknown_effects),
            EffectSettlementClass::Pending => Some(&mut self.pending_effects),
            EffectSettlementClass::Settled => None,
        };
        if let Some(source) = source
            && !source.remove(&effect.intent_id)
        {
            return Err(CoreError::Validation(format!(
                "Effect {} is missing from its settlement index",
                effect.intent_id
            )));
        }
        self.terminal_transition_effects.remove(&effect.intent_id);
        if scope_status == ScopeStatus::Open {
            let scope = self
                .open_scope_effects
                .get_mut(&effect.scope_id)
                .ok_or_else(|| {
                    CoreError::Validation(format!(
                        "open Effect {} has no open-scope index",
                        effect.intent_id
                    ))
                })?;
            scope.abort_transition_intents.remove(&effect.intent_id);
            scope.abort_blockers.remove(&effect.intent_id);
        }
        Ok(())
    }

    fn settlement(&self) -> WorldSettlementStatus {
        if !self.governance_effects.is_empty() {
            WorldSettlementStatus::GovernanceRequired
        } else if !self.unknown_effects.is_empty() {
            WorldSettlementStatus::Unknown
        } else if !self.pending_effects.is_empty() {
            WorldSettlementStatus::Pending
        } else {
            WorldSettlementStatus::Settled
        }
    }

    fn unsettled_effect_ids(&self) -> impl Iterator<Item = &String> {
        self.governance_effects
            .iter()
            .chain(&self.unknown_effects)
            .chain(&self.pending_effects)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectSettlementClass {
    Governance,
    Unknown,
    Pending,
    Settled,
}

pub(crate) fn effect_settlement_class(effect: &EffectProjection) -> EffectSettlementClass {
    if effect.reconciliation == ReconciliationState::GovernanceRequired {
        EffectSettlementClass::Governance
    } else if effect.outcome == WorldOutcome::Unknown {
        EffectSettlementClass::Unknown
    } else if effect.phase != EffectPhase::CancelledBeforeRelease
        && effect.outcome == WorldOutcome::Unobserved
    {
        EffectSettlementClass::Pending
    } else {
        EffectSettlementClass::Settled
    }
}

pub(crate) fn needs_terminal_transition(effect: &EffectProjection) -> bool {
    (effect.phase == EffectPhase::DispatchStarted && effect.outcome == WorldOutcome::Unobserved)
        || !matches!(
            effect.phase,
            EffectPhase::DispatchStarted | EffectPhase::CancelledBeforeRelease
        )
}

pub(crate) fn needs_scope_abort_transition(effect: &EffectProjection) -> bool {
    matches!(effect.phase, EffectPhase::Admitted | EffectPhase::Prepared)
}

pub(crate) fn blocks_scope_abort(effect: &EffectProjection) -> bool {
    effect.profile.mutation == MutationKind::Mutating
        && matches!(
            effect.phase,
            EffectPhase::ReleaseAuthorized | EffectPhase::DispatchStarted
        )
}

pub(crate) fn terminalized_effect(effect: &EffectProjection) -> EffectProjection {
    let mut next = effect.clone();
    match (next.phase, next.outcome) {
        (EffectPhase::DispatchStarted, WorldOutcome::Unobserved) => {
            next.outcome = WorldOutcome::Unknown;
            next.reconciliation = reconciliation_state_for_unknown(&next.profile);
        }
        (EffectPhase::DispatchStarted, _) => {}
        _ => {
            next.phase = EffectPhase::CancelledBeforeRelease;
            next.outcome = WorldOutcome::NotApplied;
            next.reconciliation = ReconciliationState::Resolved;
        }
    }
    next
}

impl RunProjection {
    /// Current stale-action protection token.
    pub fn precondition_token(&self) -> String {
        format!("pre:{}:{}", self.epoch, self.last_event)
    }

    pub(crate) fn active_attempt_id(&self) -> Option<&str> {
        self.derived.active_attempt.as_deref()
    }

    pub(crate) fn unsettled_effect_ids(&self) -> impl Iterator<Item = &String> {
        self.derived.unsettled_effect_ids()
    }

    pub(crate) fn scope_mutating_intent_ids(&self, scope_id: &str) -> Option<&[String]> {
        self.derived
            .open_scope_effects
            .get(scope_id)
            .map(|scope| scope.mutating_intent_order.as_slice())
    }

    pub(crate) const fn committed_effect_count(&self) -> u64 {
        self.derived.committed_effect_count
    }
}

/// Canonical Run execution status.
///
/// Waiting and readiness belong to the durable Continuation. This axis records
/// only whether Run execution may continue and, when terminal, why it stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunExecutionStatus {
    /// New semantic operations may still be admitted.
    Active,
    /// Execution produced its declared Result after every Effect settled.
    Completed,
    /// Execution stopped with one typed failure.
    Failed {
        /// Authoritative failure classification and evidence.
        failure: RunFailure,
    },
    /// Execution stopped after an admitted semantic cancellation.
    Cancelled {
        /// Content-addressed cancellation reason.
        reason: ArtifactRef,
    },
}

/// Classification of a terminal Run failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunFailureClass {
    /// A component returned its declared application failure outcome.
    DeclaredFailure,
    /// The selected implementation violated its runtime contract.
    RuntimeDefect,
    /// The selected implementation could not run on its admitted substrate.
    Substrate,
}

/// One typed terminal Run failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunFailure {
    /// Failure authority class.
    pub class: RunFailureClass,
    /// Stable machine-readable failure code.
    pub code: String,
    /// Immutable structured or display detail.
    pub detail: ArtifactRef,
}

impl RunFailure {
    /// Validate the bounded code and immutable detail reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the code grammar or detail reference is invalid.
    pub fn verify(&self) -> Result<()> {
        validate_failure_code(&self.code)?;
        self.detail.validate()
    }
}

/// Validate the one canonical machine-readable failure-code grammar.
///
/// # Errors
///
/// Returns an error when `code` is not lowercase ASCII with the closed
/// `[a-z][a-z0-9_]{0,199}` grammar.
pub fn validate_failure_code(code: &str) -> Result<()> {
    let valid = !code.is_empty()
        && code.len() <= 200
        && code.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || (index > 0 && (byte.is_ascii_digit() || byte == b'_'))
        });
    if valid {
        Ok(())
    } else {
        Err(CoreError::Validation(
            "failure code must match [a-z][a-z0-9_]{0,199}".to_owned(),
        ))
    }
}
/// External-world settlement on a separate Run axis.
///
/// Failed and cancelled execution may retain unsettled world state for later
/// reconciliation; completed execution requires `Settled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorldSettlementStatus {
    /// Every admitted intent is authoritatively settled or cancelled pre-release.
    Settled,
    /// At least one admitted intent has not started dispatch or settled yet.
    Pending,
    /// At least one dispatched intent has an ambiguous world outcome.
    Unknown,
    /// At least one ambiguous intent requires governance.
    GovernanceRequired,
}

/// Scope projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeProjection {
    /// Scope identity.
    pub scope_id: String,
    /// Optional parent.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_scope: Option<String>,
    /// Invocation that opened the scope, or the entry invocation for root.
    pub invocation_id: String,
    /// Entry-rooted invocation path.
    pub invocation_path: Vec<InvocationPathSegment>,
    /// Definition containing this scope's body.
    pub definition_id: String,
    /// Exact Region path of this scope's body.
    pub region_path: Vec<usize>,
    /// Stable scope site, absent only for the synthetic root scope.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub site_id: Option<String>,
    /// Scope lifecycle state.
    pub status: ScopeStatus,
    /// Effects admitted in the scope.
    pub intents: BTreeSet<String>,
    /// The same Effect identities in exact proposal order. This lineage is the
    /// paging authority for bounded scope finalization.
    pub intent_order: Vec<String>,
}

/// Scope lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeStatus {
    /// Scope accepts work.
    Open,
    /// Internal state/evidence was committed and the scope is closed.
    ClosedCommitted,
    /// Overlay and unreleased mutation were discarded.
    ClosedAborted,
}

/// One exact invoke edge in an entry-rooted dynamic invocation path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationPathSegment {
    /// Stable invoke operation site.
    pub site_id: String,
    /// Lexical Region containing the invoke site.
    pub region_path: Vec<usize>,
    /// Dynamic scope active when the invoke occurred.
    pub scope_id: String,
}

/// Effect control phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectPhase {
    /// Intent was admitted.
    Admitted,
    /// Payload and evidence were prepared.
    Prepared,
    /// Governing policy authorized release.
    ReleaseAuthorized,
    /// External dispatch began.
    DispatchStarted,
    /// Unreleased effect was cancelled by scope abort.
    CancelledBeforeRelease,
}

/// Observed external-world outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorldOutcome {
    /// No external-world observation exists yet.
    Unobserved,
    /// The external action occurred.
    Applied,
    /// The external action did not occur.
    NotApplied,
    /// Dispatch occurred but the outcome cannot currently be determined.
    Unknown,
}

/// Reconciliation result transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationResolution {
    /// Observation proved the action occurred.
    ResolvedApplied,
    /// Observation proved the action did not occur.
    ResolvedNotApplied,
    /// The outcome remains unknown and may be queried again.
    StillUnknown,
    /// Automatic resolution is unavailable.
    GovernanceRequired,
}

/// Reconciliation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationState {
    /// No reconciliation is required for the observed outcome.
    NotRequired,
    /// Unknown outcome awaits reconciliation.
    Pending,
    /// Unknown outcome was authoritatively resolved.
    Resolved,
    /// Governance must decide how to proceed.
    GovernanceRequired,
}

/// Availability of the exact implementation pinned by an effect occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectExecutionAvailability {
    /// The admitted implementation can be addressed by the active host.
    Available,
    /// The admitted implementation is unavailable; no different binding may
    /// realize this occurrence.
    Unavailable,
}

/// Effect projection with immutable occurrence binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectProjection {
    /// Structural intent identity.
    pub intent_id: String,
    /// Immutable Plan which defined this occurrence.
    pub origin_plan_id: String,
    /// Owning scope.
    pub scope_id: String,
    /// Exact dynamic invocation identity.
    pub invocation_id: String,
    /// Entry-rooted invocation path proving the dynamic invocation.
    pub invocation_path: Vec<InvocationPathSegment>,
    /// Definition containing the effect site.
    pub definition_id: String,
    /// Exact lexical Region containing the effect site.
    pub region_path: Vec<usize>,
    /// Stable reachable IR effect site.
    pub site_id: String,
    /// Intentional occurrence key declared at the site.
    pub occurrence: String,
    /// Effect identity schema version.
    pub effect_schema_version: String,
    /// Abstract operation.
    pub operation: String,
    /// Complete Plan-declared safety and recovery profile.
    pub profile: EffectProfile,
    /// Canonical arguments.
    pub args: ArtifactRef,
    /// Exact executable binding Artifact selected for this occurrence.
    pub execution_binding: ArtifactRef,
    /// Immutable historical realization.
    pub occurrence_binding: String,
    /// Availability of that exact historical realization.
    pub execution_availability: EffectExecutionAvailability,
    /// Control phase.
    pub phase: EffectPhase,
    /// World outcome.
    pub outcome: WorldOutcome,
    /// Reconciliation state.
    pub reconciliation: ReconciliationState,
}

/// Scope-transferred world-effect obligation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObligationProjection {
    /// Deterministic obligation identity.
    pub obligation_id: String,
    /// Effect intent.
    pub intent_id: String,
    /// Whether this obligation blocks normal Run completion.
    pub blocking: bool,
    /// Whether an authoritative terminal outcome is known.
    pub resolved: bool,
}

/// Fenced attempt projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptProjection {
    /// Attempt identity.
    pub attempt_id: String,
    /// Continuation identity.
    pub continuation_id: String,
    /// Immutable occurrence binding.
    pub occurrence_binding: String,
    /// Continuation generation interpreted by this Attempt.
    pub continuation_epoch: u64,
    /// Durable execution-claim fence authorizing this Attempt.
    pub execution_fence: u64,
    /// Whether the Attempt currently owns execution.
    pub active: bool,
}

/// Explicit exact-replay capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplayAvailability {
    /// All required canonical inputs are available.
    Exact,
    /// State projections remain available but complete nondeterminism does not.
    ProjectionOnly {
        /// Missing or redacted references.
        missing: Vec<String>,
    },
    /// Even the requested projection cannot be reconstructed.
    Unavailable {
        /// Stable explanation.
        reason: String,
    },
}

/// Compaction witness. The Embedded profile constructs and validates this type
/// but does not delete canonical history automatically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionCertificate {
    /// Causally closed source frontier.
    pub source_frontier: Vec<String>,
    /// Digest of the summarized projection.
    pub projection_digest: String,
    /// Unresolved obligations preserved by the summary.
    pub unresolved_obligations: Vec<String>,
    /// Historical occurrence bindings retained for interpretation.
    pub occurrence_bindings: Vec<String>,
    /// Resulting replay capability.
    pub replay_availability: ReplayAvailability,
}

impl Projection {
    pub(crate) fn rebuild_derived_indexes(&mut self) -> Result<()> {
        for run in self.runs.values_mut() {
            run.derived = RunDerivedIndex::rebuild(run)?;
        }
        Ok(())
    }

    pub(crate) fn verify_execution_numbers(&self) -> Result<()> {
        for run in self.runs.values() {
            if run.epoch > MAX_EXACT_INTEGER {
                return Err(CoreError::Validation(format!(
                    "Run {} epoch exceeds the exact cross-language integer range",
                    run.run_id
                )));
            }
            for attempt in run.attempts.values() {
                if attempt.continuation_epoch > MAX_EXACT_INTEGER
                    || attempt.execution_fence == 0
                    || attempt.execution_fence > MAX_EXACT_INTEGER
                {
                    return Err(CoreError::Validation(format!(
                        "Attempt {} epoch or execution fence exceeds the exact cross-language integer range",
                        attempt.attempt_id
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn verify_world_settlement_invariants(&self) -> Result<()> {
        for run in self.runs.values() {
            let derived = derive_world_settlement(run);
            if run.world_settlement != derived {
                return Err(CoreError::Validation(format!(
                    "Run {} world settlement does not match its Effect projection",
                    run.run_id
                )));
            }
            if matches!(run.execution_status, RunExecutionStatus::Completed)
                && derived != WorldSettlementStatus::Settled
            {
                return Err(CoreError::Validation(format!(
                    "completed Run {} retains unsettled external-world Effects",
                    run.run_id
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn verify_reducer_invariants(&self) -> Result<()> {
        self.verify_execution_numbers()?;
        self.verify_world_settlement_invariants()?;
        for (run_id, run) in &self.runs {
            if run_id != &run.run_id {
                return Err(CoreError::Validation(format!(
                    "Run projection key {run_id} does not match Run {}",
                    run.run_id
                )));
            }
            if run.plan_lineage.is_empty()
                || run.binding_lineage.is_empty()
                || run.last_event.is_empty()
                || run.plan_lineage.first() != Some(&run.initial_plan)
                || run.plan_lineage.last() != Some(&run.current_plan)
                || run.binding_lineage.first() != Some(&run.initial_binding_context)
                || run.binding_lineage.last() != Some(&run.current_binding_context)
            {
                return Err(CoreError::Validation(format!(
                    "Run {run_id} has incomplete canonical lineage or frontier evidence"
                )));
            }
            if let Some(result) = &run.result {
                result.validate()?;
            }
            verify_scope_projection(run)?;
            verify_effect_projection(run)?;
            verify_obligation_projection(run)?;

            let mut active_attempts = 0_usize;
            for (attempt_id, attempt) in &run.attempts {
                crate::validate_content_id("Attempt", attempt_id)?;
                crate::validate_content_id("Attempt continuation", &attempt.continuation_id)?;
                crate::validate_content_id(
                    "Attempt occurrence binding",
                    &attempt.occurrence_binding,
                )?;
                if attempt_id != &attempt.attempt_id
                    || attempt.continuation_epoch > run.epoch
                    || (attempt.active && attempt.continuation_epoch != run.epoch)
                {
                    return Err(CoreError::Validation(format!(
                        "Attempt {attempt_id} is inconsistent with Run {run_id} execution state"
                    )));
                }
                active_attempts += usize::from(attempt.active);
            }
            if active_attempts > 1 {
                return Err(CoreError::Validation(format!(
                    "Run {run_id} has more than one active Attempt"
                )));
            }

            let has_active_attempt = active_attempts == 1;
            let has_open_scope = run
                .scopes
                .values()
                .any(|scope| scope.status == ScopeStatus::Open);
            match &run.execution_status {
                RunExecutionStatus::Active => {
                    if run.result.is_some() {
                        return Err(CoreError::Validation(format!(
                            "active Run {run_id} carries a terminal Result"
                        )));
                    }
                }
                RunExecutionStatus::Completed => {
                    if has_active_attempt || has_open_scope {
                        return Err(CoreError::Validation(format!(
                            "completed Run {run_id} retains an active Attempt or open scope"
                        )));
                    }
                }
                RunExecutionStatus::Failed { failure } => {
                    failure.verify()?;
                    verify_terminal_run(run, has_active_attempt, has_open_scope, "failed")?;
                }
                RunExecutionStatus::Cancelled { reason } => {
                    reason.validate()?;
                    verify_terminal_run(run, has_active_attempt, has_open_scope, "cancelled")?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn verify_derived_indexes(&self) -> Result<()> {
        for (run_id, run) in &self.runs {
            if run.derived.initialized && run.derived != RunDerivedIndex::rebuild(run)? {
                return Err(CoreError::Validation(format!(
                    "Run {run_id} derived reducer index does not match canonical projection"
                )));
            }
        }
        Ok(())
    }

    /// Apply one Machine-authorized canonical event.
    ///
    /// This reducer is crate-private because a standalone Event cannot prove
    /// its sealed Plan or retained Artifact authority. `Machine` performs those
    /// checks before invoking the pure projection transition.
    pub(crate) fn apply_event(&mut self, event: &Event) -> Result<()> {
        ensure_projection_derived_index(self, event)?;
        if apply_projection_global_event(self, event)? {
            return Ok(());
        }

        let run = self
            .runs
            .get_mut(&event.run_id)
            .ok_or_else(|| CoreError::NotFound(format!("Run {} does not exist", event.run_id)))?;
        verify_run_event_gate(&run.execution_status, &event.payload, &event.run_id)?;

        match &event.payload {
            EventPayload::RunStarted { .. } | EventPayload::FactRecorded { .. } => {}
            EventPayload::AttemptStarted {
                attempt_id,
                continuation_id,
                occurrence_binding,
                continuation_epoch,
                execution_fence,
            } => {
                apply_attempt_started(
                    run,
                    &event.run_id,
                    attempt_id,
                    continuation_id,
                    occurrence_binding,
                    *continuation_epoch,
                    *execution_fence,
                )?;
            }
            EventPayload::AttemptYielded {
                attempt_id,
                continuation_epoch,
                execution_fence,
            } => {
                apply_attempt_yielded(run, attempt_id, *continuation_epoch, *execution_fence)?;
            }
            EventPayload::EpochAdvanced { epoch } => {
                apply_epoch_advanced(run, *epoch)?;
            }
            EventPayload::ScopeOpened { .. } => {
                apply_scope_opened(run, &event.payload)?;
            }
            EventPayload::EffectProposed { .. } => {
                apply_effect_proposed(run, event)?;
            }
            EventPayload::EffectTransitioned {
                intent_id,
                transition,
            } => {
                apply_effect_transitioned(run, intent_id, transition)?;
            }
            EventPayload::ScopeCommitted {
                scope_id,
                obligation_count,
                obligation_commitment,
            } => {
                apply_scope_committed(run, scope_id, *obligation_count, obligation_commitment)?;
            }
            EventPayload::ScopeAborted { scope_id } => {
                apply_scope_aborted(run, scope_id)?;
            }
            EventPayload::BindingUpdated { previous, current } => {
                apply_binding_updated(run, previous, current)?;
            }
            EventPayload::RunMigrated {
                from_plan,
                to_plan,
                from_binding,
                to_binding,
                target_epoch,
                ..
            } => {
                apply_run_migrated(
                    run,
                    from_plan,
                    to_plan,
                    from_binding,
                    to_binding,
                    *target_epoch,
                )?;
            }
            EventPayload::RunCompleted { result } => {
                apply_run_completed(run, result.as_ref())?;
            }
            EventPayload::RunFailed { failure, epoch } => {
                terminate_run_execution(run, *epoch)?;
                run.execution_status = RunExecutionStatus::Failed {
                    failure: failure.clone(),
                };
            }
            EventPayload::RunCancelled { reason, epoch } => {
                terminate_run_execution(run, *epoch)?;
                run.execution_status = RunExecutionStatus::Cancelled {
                    reason: reason.clone(),
                };
            }
        }
        run.last_event.clone_from(&event.event_id);
        Ok(())
    }

    /// Deterministic digest of the complete rebuildable projection.
    ///
    /// # Errors
    ///
    /// Returns an encoding error when the projection cannot be serialized
    /// under Core's canonical JSON contract.
    pub fn digest(&self) -> Result<String> {
        canonical_digest(self)
    }
}

fn ensure_projection_derived_index(projection: &mut Projection, event: &Event) -> Result<()> {
    if let Some(run) = projection.runs.get_mut(&event.run_id)
        && !run.derived.initialized
    {
        run.derived = RunDerivedIndex::rebuild(run)?;
    }
    Ok(())
}

fn apply_projection_global_event(projection: &mut Projection, event: &Event) -> Result<bool> {
    match &event.payload {
        EventPayload::RunStarted {
            plan_id,
            entry_definition,
            binding_context,
            ..
        } => {
            apply_run_started(
                projection,
                event,
                plan_id,
                entry_definition,
                binding_context,
            )?;
            Ok(true)
        }
        EventPayload::FactRecorded { key, value } => {
            if let Some(existing) = projection.facts.get(key) {
                if existing != value {
                    return Err(CoreError::IllegalTransition(format!(
                        "fact {key:?} already has a different value"
                    )));
                }
            } else {
                projection.facts.insert(key.clone(), value.clone());
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn apply_run_started(
    projection: &mut Projection,
    event: &Event,
    plan_id: &str,
    entry_definition: &str,
    binding_context: &str,
) -> Result<()> {
    if projection.runs.contains_key(&event.run_id) {
        return Err(CoreError::IllegalTransition(format!(
            "Run {} already exists",
            event.run_id
        )));
    }
    let invocation_id = plan_invocation_id(&event.run_id, plan_id, entry_definition, &[])?;
    let root_scope = ScopeProjection {
        scope_id: ROOT_SCOPE_ID.to_owned(),
        parent_scope: None,
        invocation_id,
        invocation_path: Vec::new(),
        definition_id: entry_definition.to_owned(),
        region_path: Vec::new(),
        site_id: None,
        status: ScopeStatus::Open,
        intents: BTreeSet::new(),
        intent_order: Vec::new(),
    };
    projection.runs.insert(
        event.run_id.clone(),
        RunProjection {
            run_id: event.run_id.clone(),
            initial_plan: plan_id.to_owned(),
            current_plan: plan_id.to_owned(),
            plan_lineage: vec![plan_id.to_owned()],
            initial_binding_context: binding_context.to_owned(),
            current_binding_context: binding_context.to_owned(),
            binding_lineage: vec![binding_context.to_owned()],
            epoch: 0,
            execution_status: RunExecutionStatus::Active,
            world_settlement: WorldSettlementStatus::Settled,
            scopes: BTreeMap::from([(ROOT_SCOPE_ID.to_owned(), root_scope)]),
            effects: BTreeMap::new(),
            obligations: BTreeMap::new(),
            attempts: BTreeMap::new(),
            result: None,
            last_event: event.event_id.clone(),
            derived: RunDerivedIndex {
                initialized: true,
                open_scope_ids: BTreeSet::from([ROOT_SCOPE_ID.to_owned()]),
                open_scope_effects: BTreeMap::from([(
                    ROOT_SCOPE_ID.to_owned(),
                    OpenScopeEffectIndex::default(),
                )]),
                ..RunDerivedIndex::default()
            },
        },
    );
    Ok(())
}

fn verify_attempt_numbers(continuation_epoch: u64, execution_fence: u64) -> Result<()> {
    if continuation_epoch > MAX_EXACT_INTEGER
        || execution_fence == 0
        || execution_fence > MAX_EXACT_INTEGER
    {
        return Err(CoreError::Validation(
            "attempt epoch and execution fence must use the exact cross-language integer range"
                .to_owned(),
        ));
    }
    Ok(())
}

fn apply_attempt_started(
    run: &mut RunProjection,
    run_id: &str,
    attempt_id: &str,
    continuation_id: &str,
    occurrence_binding: &str,
    continuation_epoch: u64,
    execution_fence: u64,
) -> Result<()> {
    if continuation_epoch != run.epoch {
        return Err(CoreError::IllegalTransition(format!(
            "attempt continuation epoch {continuation_epoch} does not match Run epoch {}",
            run.epoch
        )));
    }
    verify_attempt_numbers(continuation_epoch, execution_fence)?;
    if run.attempts.contains_key(attempt_id) {
        return Err(CoreError::IllegalTransition(format!(
            "attempt {attempt_id} already exists"
        )));
    }
    if run.derived.active_attempt.is_some() {
        return Err(CoreError::IllegalTransition(format!(
            "Run {run_id} already has an active Attempt"
        )));
    }
    run.attempts.insert(
        attempt_id.to_owned(),
        AttemptProjection {
            attempt_id: attempt_id.to_owned(),
            continuation_id: continuation_id.to_owned(),
            occurrence_binding: occurrence_binding.to_owned(),
            continuation_epoch,
            execution_fence,
            active: true,
        },
    );
    run.derived.active_attempt = Some(attempt_id.to_owned());
    Ok(())
}

fn apply_attempt_yielded(
    run: &mut RunProjection,
    attempt_id: &str,
    continuation_epoch: u64,
    execution_fence: u64,
) -> Result<()> {
    verify_attempt_numbers(continuation_epoch, execution_fence)?;
    let attempt = run
        .attempts
        .get_mut(attempt_id)
        .ok_or_else(|| CoreError::NotFound(format!("attempt {attempt_id} does not exist")))?;
    if !attempt.active
        || attempt.continuation_epoch != continuation_epoch
        || attempt.execution_fence != execution_fence
    {
        return Err(CoreError::IllegalTransition(format!(
            "attempt {attempt_id} is stale or inactive"
        )));
    }
    attempt.active = false;
    run.derived.active_attempt = None;
    Ok(())
}

fn apply_epoch_advanced(run: &mut RunProjection, epoch: u64) -> Result<()> {
    let expected = run
        .epoch
        .checked_add(1)
        .filter(|value| *value <= MAX_EXACT_INTEGER)
        .ok_or_else(|| CoreError::IllegalTransition("Run epoch overflowed".to_owned()))?;
    if epoch != expected {
        return Err(CoreError::IllegalTransition(format!(
            "epoch must advance from {} to {expected}; received {epoch}",
            run.epoch
        )));
    }
    run.epoch = epoch;
    if let Some(attempt_id) = run.derived.active_attempt.take()
        && let Some(attempt) = run.attempts.get_mut(&attempt_id)
    {
        attempt.active = false;
    }
    Ok(())
}

fn apply_scope_opened(run: &mut RunProjection, payload: &EventPayload) -> Result<()> {
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
    if run.scopes.contains_key(scope_id) {
        return Err(CoreError::IllegalTransition(format!(
            "scope {scope_id} already exists"
        )));
    }
    let parent = run.scopes.get(parent_scope).ok_or_else(|| {
        CoreError::NotFound(format!("parent scope {parent_scope} does not exist"))
    })?;
    if parent.status != ScopeStatus::Open {
        return Err(CoreError::IllegalTransition(format!(
            "parent scope {parent_scope} is not open"
        )));
    }
    run.scopes.insert(
        scope_id.clone(),
        ScopeProjection {
            scope_id: scope_id.clone(),
            parent_scope: Some(parent_scope.clone()),
            invocation_id: invocation_id.clone(),
            invocation_path: invocation_path.clone(),
            definition_id: definition_id.clone(),
            region_path: region_path.clone(),
            site_id: Some(site_id.clone()),
            status: ScopeStatus::Open,
            intents: BTreeSet::new(),
            intent_order: Vec::new(),
        },
    );
    index_opened_scope(run, scope_id)
}

fn apply_effect_proposed(run: &mut RunProjection, event: &Event) -> Result<()> {
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
    } = &event.payload
    else {
        return Err(CoreError::Validation(
            "Effect-proposal reducer received another Event payload".to_owned(),
        ));
    };
    validate_effect_proposal_header(
        run,
        intent_id,
        origin_plan_id,
        effect_schema_version,
        execution_binding,
    )?;
    let scope = run
        .scopes
        .get_mut(scope_id)
        .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))?;
    if scope.status != ScopeStatus::Open {
        return Err(CoreError::IllegalTransition(format!(
            "scope {scope_id} is not open"
        )));
    }
    let expected = effect_intent_id(&EffectIntentIdentityInput {
        run_id: &event.run_id,
        plan_id: &run.current_plan,
        invocation_id,
        site_id,
        scope_id,
        occurrence,
        args,
        effect_schema_version,
    })?;
    if *intent_id != expected {
        return Err(CoreError::IdentityMismatch(format!(
            "effect intent {intent_id} does not match {expected}"
        )));
    }
    scope.intents.insert(intent_id.clone());
    scope.intent_order.push(intent_id.clone());
    let effect = EffectProjection {
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
        execution_availability: EffectExecutionAvailability::Available,
        phase: EffectPhase::Admitted,
        outcome: WorldOutcome::Unobserved,
        reconciliation: ReconciliationState::NotRequired,
    };
    run.derived.register_effect(&effect, ScopeStatus::Open)?;
    run.effects.insert(intent_id.clone(), effect);
    update_world_settlement(run);
    Ok(())
}

fn validate_effect_proposal_header(
    run: &RunProjection,
    intent_id: &str,
    origin_plan_id: &str,
    effect_schema_version: &str,
    execution_binding: &ArtifactRef,
) -> Result<()> {
    if run.effects.contains_key(intent_id) {
        return Err(CoreError::IllegalTransition(format!(
            "effect intent {intent_id} already exists"
        )));
    }
    if origin_plan_id != run.current_plan {
        return Err(CoreError::Validation(format!(
            "effect intent {intent_id} origin Plan does not match the Run current Plan"
        )));
    }
    if effect_schema_version != EFFECT_SCHEMA_VERSION {
        return Err(CoreError::Validation(format!(
            "effect intent {intent_id} has unsupported schema version {effect_schema_version:?}"
        )));
    }
    if execution_binding.kind != EXECUTION_BINDING_ARTIFACT_KIND
        || execution_binding.artifact_id != run.current_binding_context
    {
        return Err(CoreError::Validation(format!(
            "effect intent {intent_id} execution binding does not match the Run current binding"
        )));
    }
    Ok(())
}

fn apply_effect_transitioned(
    run: &mut RunProjection,
    intent_id: &str,
    transition: &EffectTransition,
) -> Result<()> {
    let previous = run
        .effects
        .get(intent_id)
        .cloned()
        .ok_or_else(|| CoreError::NotFound(format!("effect {intent_id} does not exist")))?;
    let scope_status = run
        .scopes
        .get(&previous.scope_id)
        .ok_or_else(|| CoreError::NotFound(format!("scope {} does not exist", previous.scope_id)))?
        .status;
    let mut next = previous.clone();
    apply_effect_transition(&mut next, scope_status, transition)?;
    run.derived.remove_effect_state(&previous, scope_status)?;
    run.derived.add_effect_state(&next, scope_status)?;
    run.effects.insert(intent_id.to_owned(), next);
    update_obligation(run, intent_id)?;
    update_world_settlement(run);
    Ok(())
}

fn open_scope_close_index(run: &RunProjection, scope_id: &str) -> Result<OpenScopeEffectIndex> {
    let scope = run
        .scopes
        .get(scope_id)
        .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))?;
    if scope.status != ScopeStatus::Open {
        return Err(CoreError::IllegalTransition(format!(
            "scope {scope_id} is not open"
        )));
    }
    if run
        .derived
        .open_descendants
        .get(scope_id)
        .copied()
        .unwrap_or(0)
        != 0
    {
        return Err(CoreError::IllegalTransition(format!(
            "scope {scope_id} has an open child scope"
        )));
    }
    run.derived
        .open_scope_effects
        .get(scope_id)
        .cloned()
        .ok_or_else(|| {
            CoreError::Validation(format!("scope {scope_id} has no open-scope Effect index"))
        })
}

fn derived_scope_obligations(
    run: &RunProjection,
    scope_id: &str,
    scope_index: &OpenScopeEffectIndex,
) -> Result<BTreeMap<String, ObligationProjection>> {
    let mut expected = BTreeMap::new();
    for intent_id in &scope_index.mutating_intent_order {
        let effect = run
            .effects
            .get(intent_id)
            .ok_or_else(|| CoreError::NotFound(format!("effect {intent_id} does not exist")))?;
        let obligation = crate::machine::obligation_for_effect(effect)?;
        if expected
            .insert(obligation.obligation_id.clone(), obligation)
            .is_some()
        {
            return Err(CoreError::Validation(format!(
                "scope {scope_id} repeats a derived obligation"
            )));
        }
    }
    Ok(expected)
}

fn apply_scope_committed(
    run: &mut RunProjection,
    scope_id: &str,
    obligation_count: u64,
    obligation_commitment: &str,
) -> Result<()> {
    let scope_index = open_scope_close_index(run, scope_id)?;
    let expected = derived_scope_obligations(run, scope_id, &scope_index)?;
    let expected_order = scope_index
        .mutating_intent_order
        .iter()
        .map(|intent_id| {
            let id = effect_obligation_id(intent_id)?;
            expected
                .get(&id)
                .cloned()
                .ok_or_else(|| CoreError::NotFound(format!("obligation {id} does not exist")))
        })
        .collect::<Result<Vec<_>>>()?;
    let (expected_count, expected_commitment) =
        crate::machine::scope_obligation_summary(&expected_order)?;
    if obligation_count != expected_count || obligation_commitment != expected_commitment {
        return Err(CoreError::IllegalTransition(format!(
            "scope {scope_id} declared an inexact effect obligation summary"
        )));
    }
    run.scopes
        .get_mut(scope_id)
        .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))?
        .status = ScopeStatus::ClosedCommitted;
    index_closed_scope(run, scope_id, true)?;
    insert_scope_obligations(run, scope_id, &scope_index, expected)
}

fn insert_scope_obligations(
    run: &mut RunProjection,
    scope_id: &str,
    scope_index: &OpenScopeEffectIndex,
    expected: BTreeMap<String, ObligationProjection>,
) -> Result<()> {
    for obligation in expected.into_values() {
        if !scope_index.mutating_intents.contains(&obligation.intent_id) {
            return Err(CoreError::IllegalTransition(format!(
                "obligation {} does not belong to scope {scope_id}",
                obligation.obligation_id
            )));
        }
        if run
            .obligations
            .insert(obligation.obligation_id.clone(), obligation.clone())
            .is_some()
        {
            return Err(CoreError::IllegalTransition(format!(
                "obligation {} already exists",
                obligation.obligation_id
            )));
        }
        if !run
            .derived
            .obligation_by_intent
            .entry(obligation.intent_id.clone())
            .or_default()
            .insert(obligation.obligation_id.clone())
        {
            return Err(CoreError::Validation(format!(
                "obligation {} already exists in the derived index",
                obligation.obligation_id
            )));
        }
        if obligation.blocking
            && !obligation.resolved
            && !run
                .derived
                .unresolved_blocking_obligations
                .insert(obligation.obligation_id.clone())
        {
            return Err(CoreError::Validation(format!(
                "unresolved obligation {} already exists in the derived index",
                obligation.obligation_id
            )));
        }
    }
    Ok(())
}

fn apply_scope_aborted(run: &mut RunProjection, scope_id: &str) -> Result<()> {
    let scope_index = open_scope_close_index(run, scope_id)?;
    if !scope_index.abort_blockers.is_empty() {
        return Err(CoreError::IllegalTransition(format!(
            "scope {scope_id} cannot abort after effect release"
        )));
    }
    for intent_id in &scope_index.abort_transition_intents {
        let previous = run
            .effects
            .get(intent_id)
            .cloned()
            .ok_or_else(|| CoreError::NotFound(format!("effect {intent_id} does not exist")))?;
        let mut next = previous.clone();
        next.phase = EffectPhase::CancelledBeforeRelease;
        next.outcome = WorldOutcome::NotApplied;
        next.reconciliation = ReconciliationState::Resolved;
        run.derived
            .remove_effect_state(&previous, ScopeStatus::Open)?;
        run.derived.add_effect_state(&next, ScopeStatus::Open)?;
        run.effects.insert(intent_id.clone(), next);
    }
    run.scopes
        .get_mut(scope_id)
        .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))?
        .status = ScopeStatus::ClosedAborted;
    index_closed_scope(run, scope_id, false)?;
    update_world_settlement(run);
    Ok(())
}

fn apply_binding_updated(run: &mut RunProjection, previous: &str, current: &str) -> Result<()> {
    if run.current_binding_context != previous {
        return Err(CoreError::IllegalTransition(format!(
            "binding context changed from expected {previous}"
        )));
    }
    current.clone_into(&mut run.current_binding_context);
    run.binding_lineage.push(current.to_owned());
    Ok(())
}

fn apply_run_migrated(
    run: &mut RunProjection,
    from_plan: &str,
    to_plan: &str,
    from_binding: &str,
    to_binding: &str,
    target_epoch: u64,
) -> Result<()> {
    if run.current_plan != from_plan
        || run.current_binding_context != from_binding
        || run.derived.active_attempt.is_some()
        || run.epoch.checked_add(1) != Some(target_epoch)
        || target_epoch > MAX_EXACT_INTEGER
    {
        return Err(CoreError::IllegalTransition(
            "Run migration does not match a quiescent current Plan and binding".to_owned(),
        ));
    }
    to_plan.clone_into(&mut run.current_plan);
    to_binding.clone_into(&mut run.current_binding_context);
    run.plan_lineage.push(to_plan.to_owned());
    run.binding_lineage.push(to_binding.to_owned());
    run.epoch = target_epoch;
    Ok(())
}

fn apply_run_completed(run: &mut RunProjection, result: Option<&ArtifactRef>) -> Result<()> {
    if run.derived.active_attempt.is_some() {
        return Err(CoreError::IllegalTransition(
            "Run cannot complete while an Attempt remains active".to_owned(),
        ));
    }
    if !run.derived.unresolved_blocking_obligations.is_empty() {
        return Err(CoreError::IllegalTransition(
            "Run has unresolved blocking effect obligations".to_owned(),
        ));
    }
    if !run.derived.open_scope_ids.is_empty() {
        return Err(CoreError::IllegalTransition(
            "Run has an open scope".to_owned(),
        ));
    }
    if run.derived.settlement() != WorldSettlementStatus::Settled {
        return Err(CoreError::IllegalTransition(
            "Run cannot complete while an external-world Effect remains unsettled".to_owned(),
        ));
    }
    run.execution_status = RunExecutionStatus::Completed;
    run.result = result.cloned();
    Ok(())
}

fn verify_scope_projection(run: &RunProjection) -> Result<()> {
    let root = run
        .scopes
        .get(ROOT_SCOPE_ID)
        .ok_or_else(|| CoreError::Validation(format!("Run {} has no root scope", run.run_id)))?;
    if root.scope_id != ROOT_SCOPE_ID
        || root.parent_scope.is_some()
        || root.site_id.is_some()
        || !root.invocation_path.is_empty()
        || !root.region_path.is_empty()
    {
        return Err(CoreError::Validation(format!(
            "Run {} has a malformed root scope",
            run.run_id
        )));
    }

    for (scope_id, scope) in &run.scopes {
        if scope_id != &scope.scope_id {
            return Err(CoreError::Validation(format!(
                "scope projection key {scope_id} does not match {}",
                scope.scope_id
            )));
        }
        let ordered = scope.intent_order.iter().cloned().collect::<BTreeSet<_>>();
        if ordered.len() != scope.intent_order.len() || ordered != scope.intents {
            return Err(CoreError::Validation(format!(
                "scope {scope_id} Effect proposal lineage does not match its membership set"
            )));
        }
        for intent_id in &scope.intent_order {
            crate::validate_content_id("scope Effect", intent_id)?;
        }
        if scope_id == ROOT_SCOPE_ID {
            continue;
        }
        if scope.site_id.is_none() || scope.region_path.is_empty() {
            return Err(CoreError::Validation(format!(
                "scope {scope_id} has incomplete lexical authority"
            )));
        }
        let parent_id = scope
            .parent_scope
            .as_deref()
            .ok_or_else(|| CoreError::Validation(format!("scope {scope_id} has no parent")))?;
        if parent_id == scope_id || !run.scopes.contains_key(parent_id) {
            return Err(CoreError::Validation(format!(
                "scope {scope_id} has an invalid parent {parent_id}"
            )));
        }

        let mut seen = BTreeSet::from([scope_id.as_str()]);
        let mut ancestor = Some(parent_id);
        while let Some(ancestor_id) = ancestor {
            if !seen.insert(ancestor_id) {
                return Err(CoreError::Validation(format!(
                    "Run {} scope tree contains a cycle at {ancestor_id}",
                    run.run_id
                )));
            }
            let ancestor_scope = run.scopes.get(ancestor_id).ok_or_else(|| {
                CoreError::Validation(format!(
                    "scope {scope_id} references missing ancestor {ancestor_id}"
                ))
            })?;
            if scope.status == ScopeStatus::Open && ancestor_scope.status != ScopeStatus::Open {
                return Err(CoreError::Validation(format!(
                    "open scope {scope_id} has closed ancestor {ancestor_id}"
                )));
            }
            ancestor = ancestor_scope.parent_scope.as_deref();
        }
    }
    Ok(())
}

fn verify_effect_projection(run: &RunProjection) -> Result<()> {
    for (intent_id, effect) in &run.effects {
        if intent_id != &effect.intent_id {
            return Err(CoreError::Validation(format!(
                "Effect projection key {intent_id} does not match {}",
                effect.intent_id
            )));
        }
        let scope = run.scopes.get(&effect.scope_id).ok_or_else(|| {
            CoreError::Validation(format!(
                "Effect {intent_id} references missing scope {}",
                effect.scope_id
            ))
        })?;
        if !scope.intents.contains(intent_id) {
            return Err(CoreError::Validation(format!(
                "Effect {intent_id} is absent from owning scope {}",
                effect.scope_id
            )));
        }
        validate_effect_args_reference(&effect.args)?;
        effect.execution_binding.validate()?;
        if effect.execution_binding.kind != EXECUTION_BINDING_ARTIFACT_KIND {
            return Err(CoreError::Validation(format!(
                "Effect {intent_id} execution binding has the wrong Artifact kind"
            )));
        }
        verify_effect_reducer_state(effect)?;
        verify_effect_scope_state(effect, scope)?;
    }
    for scope in run.scopes.values() {
        for intent_id in &scope.intents {
            let effect = run.effects.get(intent_id).ok_or_else(|| {
                CoreError::Validation(format!(
                    "scope {} references missing Effect {intent_id}",
                    scope.scope_id
                ))
            })?;
            if effect.scope_id != scope.scope_id {
                return Err(CoreError::Validation(format!(
                    "scope {} claims Effect {intent_id} owned by {}",
                    scope.scope_id, effect.scope_id
                )));
            }
        }
    }
    Ok(())
}

fn verify_effect_scope_state(effect: &EffectProjection, scope: &ScopeProjection) -> Result<()> {
    let eager_observation = effect.profile.dispatch == DispatchPolicy::Eager
        && effect.profile.mutation == MutationKind::Observational;
    let released = matches!(
        effect.phase,
        EffectPhase::ReleaseAuthorized | EffectPhase::DispatchStarted
    );
    let release_is_legal = match effect.profile.dispatch {
        DispatchPolicy::Eager => eager_observation,
        DispatchPolicy::OnScopeCommit | DispatchPolicy::Explicit => {
            scope.status == ScopeStatus::ClosedCommitted
        }
    };
    let aborted_scope_is_legal = scope.status != ScopeStatus::ClosedAborted
        || effect.phase == EffectPhase::CancelledBeforeRelease
        || (eager_observation && released);
    if (effect.profile.dispatch == DispatchPolicy::Eager && !eager_observation)
        || (released && !release_is_legal)
        || !aborted_scope_is_legal
    {
        return Err(CoreError::Validation(format!(
            "Effect {} phase is incompatible with its dispatch policy and owning scope",
            effect.intent_id
        )));
    }
    Ok(())
}

pub(crate) fn verify_effect_reducer_state(effect: &EffectProjection) -> Result<()> {
    let legal = match effect.phase {
        EffectPhase::Admitted | EffectPhase::Prepared | EffectPhase::ReleaseAuthorized => {
            effect.execution_availability == EffectExecutionAvailability::Available
                && effect.outcome == WorldOutcome::Unobserved
                && effect.reconciliation == ReconciliationState::NotRequired
        }
        EffectPhase::CancelledBeforeRelease => {
            effect.outcome == WorldOutcome::NotApplied
                && effect.reconciliation == ReconciliationState::Resolved
        }
        EffectPhase::DispatchStarted => match (
            effect.execution_availability,
            effect.outcome,
            effect.reconciliation,
        ) {
            (
                EffectExecutionAvailability::Available,
                WorldOutcome::Unobserved,
                ReconciliationState::NotRequired,
            )
            | (
                EffectExecutionAvailability::Available,
                WorldOutcome::Applied | WorldOutcome::NotApplied,
                ReconciliationState::NotRequired | ReconciliationState::Resolved,
            )
            | (
                EffectExecutionAvailability::Unavailable,
                WorldOutcome::Unknown,
                ReconciliationState::GovernanceRequired,
            )
            | (
                EffectExecutionAvailability::Unavailable,
                WorldOutcome::Applied | WorldOutcome::NotApplied,
                ReconciliationState::Resolved,
            ) => true,
            (
                EffectExecutionAvailability::Available,
                WorldOutcome::Unknown,
                ReconciliationState::Pending,
            ) => matches!(
                effect.profile.reconciliation,
                ReconciliationMode::Queryable | ReconciliationMode::ExternallyAttested
            ),
            (
                EffectExecutionAvailability::Available,
                WorldOutcome::Unknown,
                ReconciliationState::GovernanceRequired,
            ) => matches!(
                effect.profile.reconciliation,
                ReconciliationMode::Human | ReconciliationMode::Impossible
            ),
            _ => false,
        },
    };
    if legal {
        Ok(())
    } else {
        Err(CoreError::Validation(format!(
            "Effect {} has a reducer-unreachable phase, outcome, reconciliation, or availability combination",
            effect.intent_id
        )))
    }
}

fn verify_obligation_projection(run: &RunProjection) -> Result<()> {
    let mut expected = BTreeMap::new();
    for effect in run.effects.values() {
        let scope = run
            .scopes
            .get(&effect.scope_id)
            .expect("Effect scope closure was verified before obligations");
        if effect.profile.mutation == MutationKind::Mutating
            && scope.status == ScopeStatus::ClosedCommitted
        {
            let obligation_id = effect_obligation_id(&effect.intent_id)?;
            expected.insert(
                obligation_id.clone(),
                ObligationProjection {
                    obligation_id,
                    intent_id: effect.intent_id.clone(),
                    blocking: true,
                    resolved: effect.phase == EffectPhase::CancelledBeforeRelease
                        || matches!(
                            effect.outcome,
                            WorldOutcome::Applied | WorldOutcome::NotApplied
                        ),
                },
            );
        }
    }
    if run.obligations != expected {
        return Err(CoreError::Validation(format!(
            "Run {} has an inexact reducer-derived Effect obligation set",
            run.run_id
        )));
    }
    Ok(())
}

fn verify_terminal_run(
    run: &RunProjection,
    has_active_attempt: bool,
    has_open_scope: bool,
    status: &str,
) -> Result<()> {
    if run.epoch == 0
        || run.result.is_some()
        || has_active_attempt
        || has_open_scope
        || run.world_settlement == WorldSettlementStatus::Pending
        || run
            .attempts
            .values()
            .any(|attempt| attempt.continuation_epoch >= run.epoch)
        || run.effects.values().any(|effect| {
            !matches!(
                (effect.phase, effect.outcome, effect.reconciliation),
                (EffectPhase::DispatchStarted, _, _)
                    | (
                        EffectPhase::CancelledBeforeRelease,
                        WorldOutcome::NotApplied,
                        ReconciliationState::Resolved
                    )
            )
        })
    {
        return Err(CoreError::Validation(format!(
            "{status} Run {} does not match terminal reducer state",
            run.run_id
        )));
    }
    Ok(())
}

pub(crate) fn verify_run_event_gate(
    status: &RunExecutionStatus,
    payload: &EventPayload,
    run_id: &str,
) -> Result<()> {
    let terminal_reconciliation = matches!(
        status,
        RunExecutionStatus::Failed { .. } | RunExecutionStatus::Cancelled { .. }
    ) && matches!(
        payload,
        EventPayload::EffectTransitioned {
            transition: EffectTransition::Reconcile(_),
            ..
        }
    );
    if *status != RunExecutionStatus::Active && !terminal_reconciliation {
        return Err(CoreError::IllegalTransition(format!(
            "Run {run_id} execution is already terminal"
        )));
    }
    Ok(())
}

fn terminate_run_execution(run: &mut RunProjection, epoch: u64) -> Result<()> {
    if epoch
        != run
            .epoch
            .checked_add(1)
            .filter(|value| *value <= MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                CoreError::IllegalTransition("Run terminal execution fence overflowed".to_owned())
            })?
    {
        return Err(CoreError::IllegalTransition(format!(
            "Run terminal execution fence must advance from {} to {}; received {epoch}",
            run.epoch,
            run.epoch + 1
        )));
    }
    run.epoch = epoch;
    if let Some(attempt_id) = run.derived.active_attempt.take() {
        run.attempts
            .get_mut(&attempt_id)
            .ok_or_else(|| {
                CoreError::Validation(format!(
                    "active Attempt index references missing Attempt {attempt_id}"
                ))
            })?
            .active = false;
    }
    let transition_effects = run.derived.terminal_transition_effects.clone();
    for intent_id in &transition_effects {
        let previous = run.effects.get(intent_id).cloned().ok_or_else(|| {
            CoreError::Validation(format!(
                "terminal Effect index references missing intent {intent_id}"
            ))
        })?;
        let scope_status = run
            .scopes
            .get(&previous.scope_id)
            .ok_or_else(|| {
                CoreError::Validation(format!(
                    "Effect {intent_id} has no owning scope {}",
                    previous.scope_id
                ))
            })?
            .status;
        let next = terminalized_effect(&previous);
        run.derived.remove_effect_state(&previous, scope_status)?;
        run.derived.add_effect_state(&next, scope_status)?;
        run.effects.insert(intent_id.clone(), next);
    }
    for intent_id in transition_effects {
        update_obligation(run, &intent_id)?;
    }
    let open_scope_ids = std::mem::take(&mut run.derived.open_scope_ids);
    for scope_id in open_scope_ids {
        run.scopes
            .get_mut(&scope_id)
            .ok_or_else(|| {
                CoreError::Validation(format!(
                    "open scope index references missing scope {scope_id}"
                ))
            })?
            .status = ScopeStatus::ClosedAborted;
    }
    run.derived.open_scope_effects.clear();
    run.derived.open_descendants.clear();
    update_world_settlement(run);
    Ok(())
}

fn update_world_settlement(run: &mut RunProjection) {
    run.world_settlement = run.derived.settlement();
}

/// Derive the one world-settlement axis from canonical Effect projections.
pub(crate) fn derive_world_settlement(run: &RunProjection) -> WorldSettlementStatus {
    if run
        .effects
        .values()
        .any(|effect| effect.reconciliation == ReconciliationState::GovernanceRequired)
    {
        WorldSettlementStatus::GovernanceRequired
    } else if run
        .effects
        .values()
        .any(|effect| effect.outcome == WorldOutcome::Unknown)
    {
        WorldSettlementStatus::Unknown
    } else if run.effects.values().any(|effect| {
        effect.phase != EffectPhase::CancelledBeforeRelease
            && effect.outcome == WorldOutcome::Unobserved
    }) {
        WorldSettlementStatus::Pending
    } else {
        WorldSettlementStatus::Settled
    }
}

pub(crate) fn apply_effect_transition(
    effect: &mut EffectProjection,
    scope_status: ScopeStatus,
    transition: &EffectTransition,
) -> Result<()> {
    match transition {
        EffectTransition::Prepare if effect.phase == EffectPhase::Admitted => {
            effect.phase = EffectPhase::Prepared;
        }
        EffectTransition::AuthorizeRelease
            if effect.phase == EffectPhase::Prepared
                && effect.execution_availability == EffectExecutionAvailability::Available
                && match effect.profile.dispatch {
                    DispatchPolicy::Eager => scope_status != ScopeStatus::ClosedAborted,
                    DispatchPolicy::OnScopeCommit | DispatchPolicy::Explicit => {
                        scope_status == ScopeStatus::ClosedCommitted
                    }
                } =>
        {
            effect.phase = EffectPhase::ReleaseAuthorized;
        }
        EffectTransition::StartDispatch
            if effect.phase == EffectPhase::ReleaseAuthorized
                && effect.execution_availability == EffectExecutionAvailability::Available =>
        {
            effect.phase = EffectPhase::DispatchStarted;
        }
        EffectTransition::Observe(outcome)
            if effect.phase == EffectPhase::DispatchStarted
                && effect.execution_availability == EffectExecutionAvailability::Available
                && effect.outcome == WorldOutcome::Unobserved
                && *outcome != WorldOutcome::Unobserved =>
        {
            effect.outcome = *outcome;
            effect.reconciliation = if *outcome == WorldOutcome::Unknown {
                reconciliation_state_for_unknown(&effect.profile)
            } else {
                ReconciliationState::NotRequired
            };
        }
        EffectTransition::Reconcile(resolution)
            if (effect.phase == EffectPhase::DispatchStarted
                && effect.outcome == WorldOutcome::Unknown
                && reconciliation_transition_allowed(
                    effect.profile.reconciliation,
                    effect.reconciliation,
                    *resolution,
                ))
                || (effect.phase == EffectPhase::DispatchStarted
                    && effect.execution_availability
                        == EffectExecutionAvailability::Unavailable
                    && effect.reconciliation == ReconciliationState::GovernanceRequired
                    && effect.outcome == WorldOutcome::Unknown
                    && matches!(
                        resolution,
                        ReconciliationResolution::ResolvedApplied
                            | ReconciliationResolution::ResolvedNotApplied
                    )) =>
        {
            match resolution {
                ReconciliationResolution::ResolvedApplied => {
                    effect.outcome = WorldOutcome::Applied;
                    effect.reconciliation = ReconciliationState::Resolved;
                }
                ReconciliationResolution::ResolvedNotApplied => {
                    effect.outcome = WorldOutcome::NotApplied;
                    effect.reconciliation = ReconciliationState::Resolved;
                }
                ReconciliationResolution::StillUnknown => {
                    effect.reconciliation = ReconciliationState::Pending;
                }
                ReconciliationResolution::GovernanceRequired => {
                    effect.reconciliation = ReconciliationState::GovernanceRequired;
                }
            }
        }
        EffectTransition::MarkUnavailable
            if effect.execution_availability == EffectExecutionAvailability::Available
                && !matches!(
                    effect.outcome,
                    WorldOutcome::Applied | WorldOutcome::NotApplied
                )
                && effect.phase != EffectPhase::CancelledBeforeRelease =>
        {
            effect.execution_availability = EffectExecutionAvailability::Unavailable;
            if effect.phase == EffectPhase::DispatchStarted {
                effect.outcome = WorldOutcome::Unknown;
                effect.reconciliation = ReconciliationState::GovernanceRequired;
            } else {
                effect.phase = EffectPhase::CancelledBeforeRelease;
                effect.outcome = WorldOutcome::NotApplied;
                effect.reconciliation = ReconciliationState::Resolved;
            }
        }
        _ => {
            return Err(CoreError::IllegalTransition(format!(
                "illegal effect transition {transition:?} from phase {:?}, outcome {:?}, reconciliation {:?}",
                effect.phase, effect.outcome, effect.reconciliation
            )));
        }
    }
    Ok(())
}

fn reconciliation_state_for_unknown(profile: &EffectProfile) -> ReconciliationState {
    match profile.reconciliation {
        ReconciliationMode::Queryable | ReconciliationMode::ExternallyAttested => {
            ReconciliationState::Pending
        }
        ReconciliationMode::Human | ReconciliationMode::Impossible => {
            ReconciliationState::GovernanceRequired
        }
    }
}

fn index_opened_scope(run: &mut RunProjection, scope_id: &str) -> Result<()> {
    let parent = run
        .scopes
        .get(scope_id)
        .and_then(|scope| scope.parent_scope.clone());
    if !run.derived.open_scope_ids.insert(scope_id.to_owned())
        || run
            .derived
            .open_scope_effects
            .insert(scope_id.to_owned(), OpenScopeEffectIndex::default())
            .is_some()
    {
        return Err(CoreError::Validation(format!(
            "open scope index already contains {scope_id}"
        )));
    }
    if let Some(parent_id) = parent {
        let count = run
            .derived
            .open_descendants
            .entry(parent_id.clone())
            .or_default();
        *count = count
            .checked_add(1)
            .filter(|value| *value <= MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                CoreError::Validation("open scope descendant count overflowed".to_owned())
            })?;
    }
    Ok(())
}

fn index_closed_scope(run: &mut RunProjection, scope_id: &str, committed: bool) -> Result<()> {
    let parent = run
        .scopes
        .get(scope_id)
        .and_then(|scope| scope.parent_scope.clone());
    if !run.derived.open_scope_ids.remove(scope_id) {
        return Err(CoreError::Validation(format!(
            "open scope index does not contain {scope_id}"
        )));
    }
    let effects = run
        .derived
        .open_scope_effects
        .remove(scope_id)
        .ok_or_else(|| {
            CoreError::Validation(format!("open scope index does not contain {scope_id}"))
        })?;
    if committed {
        run.derived.committed_effect_count = run
            .derived
            .committed_effect_count
            .checked_add(
                u64::try_from(effects.all_intents.len())
                    .map_err(|error| CoreError::Validation(error.to_string()))?,
            )
            .filter(|count| *count <= MAX_EXACT_INTEGER)
            .ok_or_else(|| CoreError::Validation("committed Effect count overflowed".to_owned()))?;
    }
    if let Some(parent_id) = parent {
        let remove = {
            let count = run
                .derived
                .open_descendants
                .get_mut(&parent_id)
                .ok_or_else(|| {
                    CoreError::Validation("open scope descendant index is incomplete".to_owned())
                })?;
            *count = count.checked_sub(1).ok_or_else(|| {
                CoreError::Validation("open scope descendant count underflowed".to_owned())
            })?;
            *count == 0
        };
        if remove {
            run.derived.open_descendants.remove(&parent_id);
        }
    }
    Ok(())
}

fn reconciliation_transition_allowed(
    mode: ReconciliationMode,
    state: ReconciliationState,
    resolution: ReconciliationResolution,
) -> bool {
    match mode {
        ReconciliationMode::Queryable | ReconciliationMode::ExternallyAttested => {
            state == ReconciliationState::Pending
                && !matches!(resolution, ReconciliationResolution::GovernanceRequired)
        }
        ReconciliationMode::Human | ReconciliationMode::Impossible => {
            state == ReconciliationState::GovernanceRequired
                && matches!(
                    resolution,
                    ReconciliationResolution::ResolvedApplied
                        | ReconciliationResolution::ResolvedNotApplied
                )
        }
    }
}

fn update_obligation(run: &mut RunProjection, intent_id: &str) -> Result<()> {
    let resolved = run.effects.get(intent_id).is_some_and(|effect| {
        effect.phase == EffectPhase::CancelledBeforeRelease
            || matches!(
                effect.outcome,
                WorldOutcome::Applied | WorldOutcome::NotApplied
            )
    });
    let obligation_ids = run
        .derived
        .obligation_by_intent
        .get(intent_id)
        .cloned()
        .unwrap_or_default();
    for obligation_id in obligation_ids {
        if let Some(obligation) = run.obligations.get_mut(&obligation_id)
            && obligation.resolved != resolved
        {
            if obligation.blocking {
                if resolved {
                    if !run
                        .derived
                        .unresolved_blocking_obligations
                        .remove(&obligation_id)
                    {
                        return Err(CoreError::Validation(format!(
                            "unresolved obligation index is missing {obligation_id}"
                        )));
                    }
                } else if !run
                    .derived
                    .unresolved_blocking_obligations
                    .insert(obligation_id.clone())
                {
                    return Err(CoreError::Validation(format!(
                        "unresolved obligation index repeats {obligation_id}"
                    )));
                }
            }
            obligation.resolved = resolved;
        }
    }
    Ok(())
}

/// Closed identity input for one intentional Effect occurrence.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct EffectIntentIdentityInput<'a> {
    /// Owning Run identity.
    pub run_id: &'a str,
    /// Exact sealed Plan identity.
    pub plan_id: &'a str,
    /// Exact structural invocation identity.
    pub invocation_id: &'a str,
    /// Effect site identity inside the Plan.
    pub site_id: &'a str,
    /// Owning Scope identity.
    pub scope_id: &'a str,
    /// Structural occurrence discriminator.
    pub occurrence: &'a str,
    /// Canonical Effect arguments.
    pub args: &'a ArtifactRef,
    /// Exact Effect schema generation.
    pub effect_schema_version: &'a str,
}

/// Derive the structural identity of one intentional Effect occurrence.
///
/// # Errors
///
/// Returns an error when the argument Artifact is not the canonical Effect
/// argument kind or when canonical identity derivation fails.
pub fn effect_intent_id(input: &EffectIntentIdentityInput<'_>) -> Result<String> {
    validate_effect_args_reference(input.args)?;
    content_id(EFFECT_INTENT_ID_DOMAIN, input)
}

fn validate_effect_args_reference(args: &ArtifactRef) -> Result<()> {
    args.validate()?;
    if args.kind != EFFECT_ARGS_ARTIFACT_KIND {
        return Err(CoreError::Validation(format!(
            "effect argument Artifact must have exact kind {EFFECT_ARGS_ARTIFACT_KIND}"
        )));
    }
    Ok(())
}

/// Derive the exact dynamic invocation identity from its entry-rooted path.
///
/// # Errors
///
/// Returns an encoding error when the identity preimage cannot be serialized
/// canonically.
pub fn plan_invocation_id(
    run_id: &str,
    plan_id: &str,
    entry_definition: &str,
    invocation_path: &[InvocationPathSegment],
) -> Result<String> {
    content_id(
        INVOCATION_ID_DOMAIN,
        &(run_id, plan_id, entry_definition, invocation_path),
    )
}

/// Derive one exact dynamic scope identity from its lexical body location.
///
/// # Errors
///
/// Returns an encoding error when the identity preimage cannot be serialized
/// canonically.
pub fn plan_scope_id(
    run_id: &str,
    plan_id: &str,
    invocation_id: &str,
    definition_id: &str,
    body_region_path: &[usize],
) -> Result<String> {
    content_id(
        SCOPE_ID_DOMAIN,
        &(
            run_id,
            plan_id,
            invocation_id,
            definition_id,
            body_region_path,
        ),
    )
}

/// Derive a stable obligation identity from an intent.
///
/// # Errors
///
/// Returns an encoding error when the intent identity cannot be serialized
/// canonically.
pub fn effect_obligation_id(intent_id: &str) -> Result<String> {
    content_id(EFFECT_OBLIGATION_ID_DOMAIN, &intent_id)
}
