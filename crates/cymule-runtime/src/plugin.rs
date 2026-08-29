use std::collections::BTreeMap;

use cymule_core::{
    MAX_ARTIFACT_BYTES, ReconciliationResolution, WorldOutcome, canonical_bytes, content_id,
    validate_content_id, validate_semantic_id,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    ExecutionBinding, ExecutionOperationKind, MAX_EXECUTION_OPERATIONS_PER_KIND, RuntimeError,
    RuntimeResult, composition::deserialize_bounded_map,
};

/// Process plugin protocol version.
pub const PLUGIN_VERSION: &str = "cymule.plugin/3";
/// Provider-side per-intent attempt authority generation.
pub const EFFECT_PROVIDER_ATTEMPT_VERSION: &str = "cymule.effect-provider-attempt/1";
/// Maximum canonical request, response, or manifest bytes in plugin/3.
pub const MAX_PLUGIN_MESSAGE_BYTES: usize = MAX_ARTIFACT_BYTES;

/// Exact provider-side attempt which participates in one intent's settlement
/// linearization ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectProviderAttempt {
    /// Frozen attempt generation.
    pub attempt_version: String,
    /// Content identity derived from the intent and retained claim.
    pub attempt_id: String,
    /// Original durable outbox claim owner.
    pub claim_owner: String,
    /// Original durable outbox claim fence.
    pub claim_epoch: u64,
}

impl EffectProviderAttempt {
    /// Derive one exact provider attempt for an already-claimed intent.
    ///
    /// # Errors
    ///
    /// Returns a runtime defect when the intent, owner, or claim epoch is not a
    /// valid exact provider-attempt authority.
    pub fn new(intent_id: &str, claim_owner: &str, claim_epoch: u64) -> RuntimeResult<Self> {
        let attempt_id = effect_provider_attempt_id(intent_id, claim_owner, claim_epoch)?;
        Ok(Self {
            attempt_version: EFFECT_PROVIDER_ATTEMPT_VERSION.to_owned(),
            attempt_id,
            claim_owner: claim_owner.to_owned(),
            claim_epoch,
        })
    }

    /// Verify the complete attempt against its semantic intent.
    ///
    /// # Errors
    ///
    /// Returns a runtime defect when any retained attempt field differs from
    /// the canonical identity derived from the intent and claim.
    pub fn verify_for(&self, intent_id: &str) -> RuntimeResult<()> {
        let expected = Self::new(intent_id, &self.claim_owner, self.claim_epoch)?;
        if self != &expected {
            return Err(RuntimeError::plugin_defect(
                "effect provider attempt does not match its intent and claim",
            ));
        }
        Ok(())
    }
}

/// Derive the sole provider attempt identity for one claimed intent.
///
/// # Errors
///
/// Returns a runtime defect when the intent or owner is invalid, the epoch is
/// outside the exact positive range, or content identity derivation fails.
pub fn effect_provider_attempt_id(
    intent_id: &str,
    claim_owner: &str,
    claim_epoch: u64,
) -> RuntimeResult<String> {
    validate_effect_intent_id(intent_id)?;
    cymule_core::validate_identity("effect claim owner", claim_owner)?;
    if claim_epoch == 0 || claim_epoch > cymule_core::MAX_EXACT_INTEGER {
        return Err(RuntimeError::plugin_defect(
            "effect provider claim epoch is outside the exact positive range",
        ));
    }
    content_id(
        EFFECT_PROVIDER_ATTEMPT_VERSION,
        &(intent_id, claim_owner, claim_epoch),
    )
    .map_err(RuntimeError::from)
}

/// Closed provider-side reconciliation action. Any reconciliation first
/// closes first-dispatch admission in the same per-intent ledger transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectReconciliationDecision {
    /// Close late dispatch admission and report the provider's current state.
    Observe,
    /// Close late dispatch admission and request an Applied terminal decision.
    ResolveApplied,
    /// Close late dispatch admission and request a `NotApplied` tombstone.
    ResolveNotApplied,
}

impl EffectReconciliationDecision {
    /// Requested terminal resolution, absent for an observation-only query.
    pub const fn requested_resolution(self) -> Option<ReconciliationResolution> {
        match self {
            Self::Observe => None,
            Self::ResolveApplied => Some(ReconciliationResolution::ResolvedApplied),
            Self::ResolveNotApplied => Some(ReconciliationResolution::ResolvedNotApplied),
        }
    }
}

/// Declared application failure returned by a component implementation.
///
/// This value is distinct from a plugin defect. Callers may branch on `code`;
/// `message` is display-only and never carries control semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginExpectedFailure {
    /// Stable application-owned failure code.
    pub code: String,
    /// Human-readable summary.
    pub message: String,
}

impl PluginExpectedFailure {
    /// Validate the bounded, machine-readable failure value.
    ///
    /// # Errors
    ///
    /// Returns a plugin defect when the code or Unicode-scalar message bound is
    /// invalid.
    pub fn verify(&self) -> RuntimeResult<()> {
        cymule_core::validate_failure_code(&self.code).map_err(|_| {
            RuntimeError::plugin_defect("plugin expected failure is not a bounded closed value")
        })?;
        if self.message.is_empty()
            || self.message.chars().count() > 2000
            || self.message.chars().any(char::is_control)
        {
            return Err(RuntimeError::plugin_defect(
                "plugin expected failure is not a bounded closed value",
            ));
        }
        Ok(())
    }
}

/// One abstract component operation advertised by a plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginOperation {
    /// Stable implementation-specific revision.
    pub implementation_revision: String,
}

/// One abstract effect implementation advertised by a plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginEffect {
    /// Stable implementation-specific revision.
    pub implementation_revision: String,
    /// Whether the adapter can authoritatively reconcile ambiguity.
    pub can_reconcile: bool,
}

fn deserialize_plugin_components<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, PluginOperation>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_bounded_map::<D, String, PluginOperation, MAX_EXECUTION_OPERATIONS_PER_KIND>(
        deserializer,
        "plugin components",
    )
}

fn deserialize_plugin_effects<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, PluginEffect>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_bounded_map::<D, String, PluginEffect, MAX_EXECUTION_OPERATIONS_PER_KIND>(
        deserializer,
        "plugin effects",
    )
}

/// Plugin capability advertisement. It does not grant authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginManifest {
    /// Protocol version.
    pub plugin_version: String,
    /// Immutable implementation identity used in occurrence bindings.
    pub implementation_id: String,
    /// Component implementations.
    #[serde(deserialize_with = "deserialize_plugin_components")]
    pub components: BTreeMap<String, PluginOperation>,
    /// Effect implementations.
    #[serde(deserialize_with = "deserialize_plugin_effects")]
    pub effects: BTreeMap<String, PluginEffect>,
}

impl PluginManifest {
    /// Validate the complete capability advertisement before it can participate
    /// in execution-binding admission.
    ///
    /// # Errors
    ///
    /// Returns a plugin defect when the protocol version, implementation, or
    /// any advertised operation revision is invalid.
    pub fn verify(&self) -> RuntimeResult<()> {
        if self.plugin_version != PLUGIN_VERSION {
            return Err(RuntimeError::plugin_defect(format!(
                "unsupported plugin version {:?}",
                self.plugin_version
            )));
        }
        validate_manifest_token("plugin implementation ID", &self.implementation_id)?;
        if self.components.len() > MAX_EXECUTION_OPERATIONS_PER_KIND
            || self.effects.len() > MAX_EXECUTION_OPERATIONS_PER_KIND
        {
            return Err(RuntimeError::PluginDefect {
                code: "plugin_manifest_limit_exceeded".to_owned(),
                message: format!(
                    "plugin manifest component/effect maps may contain at most {MAX_EXECUTION_OPERATIONS_PER_KIND} entries each"
                ),
            });
        }
        for (operation, advertised) in &self.components {
            validate_operation_id("component operation", operation)?;
            validate_manifest_token(
                "component implementation revision",
                &advertised.implementation_revision,
            )?;
        }
        for (operation, advertised) in &self.effects {
            validate_operation_id("effect operation", operation)?;
            validate_manifest_token(
                "effect implementation revision",
                &advertised.implementation_revision,
            )?;
        }
        verify_plugin_message_size("plugin manifest", self)
    }
}

/// Versioned process-plugin request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PluginRequest {
    /// Describe supported abstract operations.
    Describe,
    /// Execute one component occurrence.
    Call {
        /// Abstract component operation.
        component: String,
        /// Typed input.
        input: Value,
    },
    /// Prepare an effect without claiming external application.
    PrepareEffect {
        /// Abstract effect operation.
        operation: String,
        /// Structural intent identity.
        intent_id: String,
        /// Typed input.
        input: Value,
    },
    /// Dispatch an authorized effect occurrence.
    DispatchEffect {
        /// Abstract effect operation.
        operation: String,
        /// Structural intent identity and provider idempotency source.
        intent_id: String,
        /// Exact durable claim admitted by the provider's settlement ledger.
        attempt: EffectProviderAttempt,
        /// Typed input.
        input: Value,
    },
    /// Reconcile an unknown effect using the same occurrence binding.
    ReconcileEffect {
        /// Abstract effect operation.
        operation: String,
        /// Original structural intent identity.
        intent_id: String,
        /// Exact original claim participating in provider settlement.
        attempt: EffectProviderAttempt,
        /// Observation or requested terminal decision.
        decision: EffectReconciliationDecision,
        /// Caller-supplied terminal result evidence, null for observation-only
        /// reconciliation or a result-less decision.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        resolution_value: Option<Value>,
        /// Original typed input.
        input: Value,
    },
}

impl PluginRequest {
    /// Validate request-local identities and provider attempt authority.
    ///
    /// # Errors
    ///
    /// Returns a plugin defect when an operation, intent, attempt, or
    /// reconciliation decision/value combination is invalid.
    pub fn verify(&self) -> RuntimeResult<()> {
        match self {
            Self::Describe => Ok(()),
            Self::Call { component, .. } => validate_operation_id("component operation", component),
            Self::PrepareEffect {
                operation,
                intent_id,
                ..
            } => {
                validate_operation_id("effect operation", operation)?;
                validate_effect_intent_id(intent_id)
            }
            Self::DispatchEffect {
                operation,
                intent_id,
                attempt,
                ..
            } => {
                validate_operation_id("effect operation", operation)?;
                attempt.verify_for(intent_id)
            }
            Self::ReconcileEffect {
                operation,
                intent_id,
                attempt,
                decision,
                resolution_value,
                ..
            } => {
                validate_operation_id("effect operation", operation)?;
                attempt.verify_for(intent_id)?;
                if matches!(decision, EffectReconciliationDecision::Observe)
                    && resolution_value.is_some()
                {
                    return Err(RuntimeError::plugin_defect(
                        "observation-only reconciliation cannot carry a resolution value",
                    ));
                }
                if matches!(decision, EffectReconciliationDecision::ResolveNotApplied)
                    && resolution_value.is_some()
                {
                    return Err(RuntimeError::plugin_defect(
                        "NotApplied resolution cannot carry an Effect result",
                    ));
                }
                Ok(())
            }
        }?;
        verify_plugin_message_size("plugin request", self)
    }
}

/// Versioned process-plugin response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PluginResponse {
    /// Capability advertisement.
    Manifest {
        /// Manifest.
        manifest: PluginManifest,
    },
    /// Component result.
    CallResult {
        /// Typed output.
        value: Value,
    },
    /// A component completed with a declared application failure.
    ExpectedFailure {
        /// Closed application failure value.
        error: PluginExpectedFailure,
    },
    /// Preparation succeeded.
    Prepared,
    /// Dispatch produced an observed outcome.
    EffectResult {
        /// Exact provider attempt admitted by the per-intent ledger.
        attempt: EffectProviderAttempt,
        /// External-world observation.
        outcome: WorldOutcome,
        /// Optional typed operation result.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        value: Option<Value>,
    },
    /// Reconciliation produced a typed resolution.
    ReconciliationResult {
        /// Exact provider attempt admitted by the per-intent ledger.
        attempt: EffectProviderAttempt,
        /// Resolution.
        resolution: ReconciliationResolution,
        /// Optional typed operation result.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        value: Option<Value>,
    },
    /// The plugin reports that it could not honor the protocol correctly.
    Defect {
        /// Stable adapter code.
        code: String,
        /// Human-readable summary.
        message: String,
    },
}

impl PluginResponse {
    /// Validate response-local closed values before any request-specific or
    /// Machine transition handling.
    ///
    /// # Errors
    ///
    /// Returns a plugin defect when the response contains malformed declared
    /// failure data or a provider-authored governance escalation.
    pub fn verify(&self) -> RuntimeResult<()> {
        match self {
            Self::ExpectedFailure { error } => error.verify(),
            Self::ReconciliationResult {
                resolution: ReconciliationResolution::GovernanceRequired,
                ..
            } => Err(RuntimeError::PluginDefect {
                code: "invalid_reconciliation_resolution".to_owned(),
                message: "Effect providers cannot author governance escalation".to_owned(),
            }),
            Self::Defect { code, message } => PluginExpectedFailure {
                code: code.clone(),
                message: message.clone(),
            }
            .verify()
            .map_err(|_| RuntimeError::PluginDefect {
                code: "invalid_plugin_defect".to_owned(),
                message: "plugin defect response is not a bounded closed value".to_owned(),
            }),
            Self::Manifest { manifest } => manifest.verify(),
            Self::CallResult { .. }
            | Self::Prepared
            | Self::EffectResult { .. }
            | Self::ReconciliationResult { .. } => Ok(()),
        }?;
        verify_plugin_message_size("plugin response", self)
    }

    /// Verify the response variant and provider authority against its exact request.
    ///
    /// # Errors
    ///
    /// Returns a plugin defect when response-local validation fails, the
    /// variant does not match the request, or Effect attempt/outcome authority
    /// differs from the request.
    pub fn verify_for(&self, request: &PluginRequest) -> RuntimeResult<()> {
        self.verify()?;
        match (request, self) {
            (_, Self::Defect { .. })
            | (PluginRequest::Describe, Self::Manifest { .. })
            | (PluginRequest::Call { .. }, Self::CallResult { .. })
            | (PluginRequest::Call { .. }, Self::ExpectedFailure { .. })
            | (PluginRequest::PrepareEffect { .. }, Self::Prepared) => Ok(()),
            (
                PluginRequest::DispatchEffect {
                    intent_id,
                    attempt: expected,
                    ..
                },
                Self::EffectResult {
                    attempt,
                    outcome,
                    value,
                },
            ) => {
                attempt.verify_for(intent_id)?;
                if attempt != expected
                    || matches!(outcome, WorldOutcome::NotApplied | WorldOutcome::Unknown)
                        && value.is_some()
                {
                    return Err(RuntimeError::plugin_defect(
                        "Effect dispatch response does not match its provider attempt or outcome",
                    ));
                }
                Ok(())
            }
            (
                PluginRequest::ReconcileEffect {
                    intent_id,
                    attempt: expected,
                    ..
                },
                Self::ReconciliationResult {
                    attempt,
                    resolution,
                    value,
                },
            ) => {
                attempt.verify_for(intent_id)?;
                if attempt != expected
                    || !matches!(resolution, ReconciliationResolution::ResolvedApplied)
                        && value.is_some()
                {
                    return Err(RuntimeError::plugin_defect(
                        "Effect reconciliation response does not match its provider attempt or resolution",
                    ));
                }
                Ok(())
            }
            _ => Err(RuntimeError::plugin_defect(
                "plugin response variant does not match its request",
            )),
        }
    }
}

/// Decode one untrusted plugin/3 request under the semantic Artifact byte bound
/// before typed allocation and validation.
///
/// # Errors
///
/// Returns a stable plugin defect for an oversized, duplicate-bearing,
/// presence-losing, or otherwise malformed request.
pub fn decode_plugin_request(input: &[u8]) -> RuntimeResult<PluginRequest> {
    let request: PluginRequest = decode_plugin_message(input, "request", "invalid_plugin_request")?;
    request.verify()?;
    Ok(request)
}

/// Decode one untrusted plugin/3 response under the semantic Artifact byte
/// bound before typed allocation and validation.
///
/// # Errors
///
/// Returns a stable plugin defect for an oversized, duplicate-bearing,
/// presence-losing, or otherwise malformed response.
pub fn decode_plugin_response(input: &[u8]) -> RuntimeResult<PluginResponse> {
    let response: PluginResponse =
        decode_plugin_message(input, "response", "invalid_plugin_response")?;
    response.verify()?;
    Ok(response)
}

fn decode_plugin_message<T>(input: &[u8], label: &str, code: &'static str) -> RuntimeResult<T>
where
    T: DeserializeOwned + Serialize,
{
    if input.len() > MAX_PLUGIN_MESSAGE_BYTES {
        return Err(RuntimeError::PluginDefect {
            code: "plugin_message_too_large".to_owned(),
            message: format!(
                "plugin {label} uses {} raw bytes, above the {MAX_PLUGIN_MESSAGE_BYTES} byte bound",
                input.len()
            ),
        });
    }
    let raw =
        crate::decode_strict_json_value(input).map_err(|message| RuntimeError::PluginDefect {
            code: code.to_owned(),
            message,
        })?;
    let message: T =
        serde_json::from_value(raw.clone()).map_err(|error| RuntimeError::PluginDefect {
            code: code.to_owned(),
            message: error.to_string(),
        })?;
    let normalized =
        serde_json::to_value(&message).map_err(|error| RuntimeError::PluginDefect {
            code: code.to_owned(),
            message: error.to_string(),
        })?;
    crate::validate_json_typed_roundtrip(&raw, &normalized).map_err(|message| {
        RuntimeError::PluginDefect {
            code: code.to_owned(),
            message,
        }
    })?;
    Ok(message)
}

fn verify_plugin_message_size<T: Serialize>(label: &str, value: &T) -> RuntimeResult<()> {
    let size = canonical_bytes(value)
        .map_err(|error| RuntimeError::PluginDefect {
            code: "invalid_plugin_message_encoding".to_owned(),
            message: error.to_string(),
        })?
        .len();
    if size > MAX_PLUGIN_MESSAGE_BYTES {
        return Err(RuntimeError::PluginDefect {
            code: "plugin_message_too_large".to_owned(),
            message: format!(
                "{label} uses {size} canonical bytes, above the {MAX_PLUGIN_MESSAGE_BYTES} byte bound"
            ),
        });
    }
    Ok(())
}

/// Framework-owned proof that one selected operation passed exact binding and
/// live-manifest admission. Private fields prevent callers or plugin adapters
/// from fabricating the unchecked invocation capability.
pub struct BoundOperationAdmission<'a> {
    kind: ExecutionOperationKind,
    operation: String,
    unavailable: Option<crate::CompositionError>,
    invoke: Option<Box<dyn FnOnce(PluginRequest) -> RuntimeResult<PluginResponse> + 'a>>,
}

/// One-shot proof that an exact plugin host and immutable execution binding
/// passed live-manifest admission together.
///
/// The token owns the admitted host, so it cannot be replayed against a
/// different provider instance. Its fields and constructor stay private;
/// runtime owners consume it exactly once when they open an interpreter.
pub struct ExecutionBindingAdmission<P> {
    plugin: P,
    binding: ExecutionBinding,
}

impl<P> ExecutionBindingAdmission<P> {
    /// Consume the one-shot proof and recover the exact admitted pair.
    ///
    /// This does not expose an unchecked constructor: reconstructing the token
    /// requires running live admission again.
    #[must_use]
    pub fn into_parts(self) -> (P, ExecutionBinding) {
        (self.plugin, self.binding)
    }

    fn admitted(plugin: P, binding: ExecutionBinding) -> Self {
        Self { plugin, binding }
    }
}

impl<P: BoundPluginHost> ExecutionBindingAdmission<P> {
    /// Admit an already constructed immutable binding against this exact host.
    ///
    /// # Errors
    ///
    /// Returns a typed runtime error when the binding is invalid or the exact
    /// host does not advertise the selected implementation.
    pub fn admit(mut plugin: P, binding: ExecutionBinding) -> RuntimeResult<Self> {
        binding.verify()?;
        plugin.admit_execution_binding(&binding)?;
        Ok(Self::admitted(plugin, binding))
    }
}

impl<P: PluginHost> ExecutionBindingAdmission<P> {
    /// Describe one direct provider exactly once and derive the immutable local
    /// process binding from that same observation.
    ///
    /// # Errors
    ///
    /// Returns a typed runtime error when Describe fails or its manifest cannot
    /// form the requested immutable binding.
    pub fn for_local_process(
        plugin: P,
        implementation_revision: impl Into<String>,
    ) -> RuntimeResult<Self> {
        let implementation_revision = implementation_revision.into();
        Self::from_manifest(plugin, move |manifest| {
            ExecutionBinding::for_local_process(manifest, implementation_revision)
                .map_err(RuntimeError::from)
        })
    }

    /// Describe one direct provider exactly once and let the framework caller
    /// derive its immutable binding from that same manifest observation.
    ///
    /// # Errors
    ///
    /// Returns a typed runtime error when Describe, binding construction, or
    /// exact manifest verification fails.
    pub fn from_manifest(
        mut plugin: P,
        build: impl FnOnce(&PluginManifest) -> RuntimeResult<ExecutionBinding>,
    ) -> RuntimeResult<Self> {
        let manifest = plugin.describe()?;
        let binding = build(&manifest)?;
        binding
            .verify_single_provider_manifest(&manifest)
            .map_err(RuntimeError::from)?;
        Ok(Self::admitted(plugin, binding))
    }
}

impl BoundOperationAdmission<'_> {
    /// Whether the exact selected operation is currently realizable.
    pub const fn is_available(&self) -> bool {
        self.unavailable.is_none()
    }

    fn admitted<'a>(
        kind: ExecutionOperationKind,
        operation: &str,
        invoke: impl FnOnce(PluginRequest) -> RuntimeResult<PluginResponse> + 'a,
    ) -> BoundOperationAdmission<'a> {
        BoundOperationAdmission {
            kind,
            operation: operation.to_owned(),
            unavailable: None,
            invoke: Some(Box::new(invoke)),
        }
    }

    fn unavailable<'a>(
        kind: ExecutionOperationKind,
        operation: &str,
        error: crate::CompositionError,
    ) -> BoundOperationAdmission<'a> {
        BoundOperationAdmission {
            kind,
            operation: operation.to_owned(),
            unavailable: Some(error),
            invoke: None,
        }
    }

    /// Consume this one-shot capability and invoke the exact admitted provider.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when the operation is unavailable, the request
    /// differs from the admitted operation, invocation fails, or the response
    /// does not match the request.
    pub fn invoke(mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        if let Some(error) = self.unavailable.take() {
            return Err(RuntimeError::from(error));
        }
        let (kind, operation) = request_operation(&request)?;
        if kind != self.kind || operation != self.operation {
            return Err(RuntimeError::plugin_defect(
                "bound operation admission does not match the invocation",
            ));
        }
        request.verify()?;
        let admitted_request = request.clone();
        let invoke = self.invoke.take().ok_or_else(|| {
            RuntimeError::plugin_defect("available bound operation has no invocation authority")
        })?;
        let response = invoke(request)?;
        response.verify_for(&admitted_request)?;
        Ok(response)
    }
}

/// Abstract plugin transport.
pub trait PluginHost {
    /// Invoke one typed plugin request.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when transport, provider execution, or protocol
    /// handling fails.
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse>;

    /// Fetch and validate the plugin manifest.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when Describe fails or returns an invalid or
    /// mismatched response.
    fn describe(&mut self) -> RuntimeResult<PluginManifest> {
        let request = PluginRequest::Describe;
        let response = self.invoke(request.clone())?;
        response.verify_for(&request)?;
        match response {
            PluginResponse::Manifest { manifest } => Ok(manifest),
            PluginResponse::Defect { code, message } => Err(RuntimeError::PluginDefect {
                code: "plugin_reported_defect".to_owned(),
                message: format!("{code}: {message}"),
            }),
            response => Err(RuntimeError::plugin_defect(format!(
                "describe returned unexpected response {response:?}"
            ))),
        }
    }

    /// Verify that this host realizes the immutable execution binding.
    ///
    /// A raw host can realize exactly one selected provider. Multi-provider
    /// routing belongs to the sealed [`BoundPluginHost`] implementation.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when Describe fails or the live manifest does
    /// not realize the exact single-provider binding.
    fn verify_execution_binding(&mut self, binding: &ExecutionBinding) -> RuntimeResult<()> {
        let manifest = self.describe()?;
        binding
            .verify_single_provider_manifest(&manifest)
            .map_err(RuntimeError::from)?;
        Ok(())
    }
}

fn validate_operation_id(kind: &str, value: &str) -> RuntimeResult<()> {
    validate_semantic_id(kind, value)
        .map_err(|error| RuntimeError::plugin_defect(error.to_string()))
}

fn validate_effect_intent_id(value: &str) -> RuntimeResult<()> {
    validate_content_id("effect intent", value)
        .map_err(|error| RuntimeError::plugin_defect(error.to_string()))
}

fn validate_manifest_token(kind: &str, value: &str) -> RuntimeResult<()> {
    if value.is_empty()
        || value.chars().count() > 200
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(RuntimeError::plugin_defect(format!(
            "{kind} must contain 1..=200 non-whitespace printable Unicode scalar values"
        )));
    }
    Ok(())
}

mod bound_plugin_host {
    use super::{
        BoundOperationAdmission, ExecutionBinding, ExecutionOperationKind, PluginHost,
        RuntimeResult,
    };

    pub trait Sealed {
        fn framework_admit_execution_binding(
            &mut self,
            binding: &ExecutionBinding,
        ) -> RuntimeResult<()>;

        fn framework_admit_equivalent_operation<'a>(
            &'a mut self,
            current_binding: &ExecutionBinding,
            historical_binding: &ExecutionBinding,
            kind: ExecutionOperationKind,
            operation: &str,
        ) -> RuntimeResult<BoundOperationAdmission<'a>>;
    }

    impl<T: PluginHost> Sealed for T {
        fn framework_admit_execution_binding(
            &mut self,
            binding: &ExecutionBinding,
        ) -> RuntimeResult<()> {
            PluginHost::verify_execution_binding(self, binding)
        }

        fn framework_admit_equivalent_operation<'a>(
            &'a mut self,
            _current_binding: &ExecutionBinding,
            historical_binding: &ExecutionBinding,
            kind: ExecutionOperationKind,
            operation: &str,
        ) -> RuntimeResult<BoundOperationAdmission<'a>> {
            let manifest = PluginHost::describe(self)?;
            match historical_binding.verify_operation_manifest(kind, operation, &manifest) {
                Ok(()) => Ok(BoundOperationAdmission::admitted(
                    kind,
                    operation,
                    move |request| PluginHost::invoke(self, request),
                )),
                Err(error) => Ok(BoundOperationAdmission::unavailable(kind, operation, error)),
            }
        }
    }
}

/// Framework-owned bound invocation sequence.
///
/// This trait is sealed: raw [`PluginHost`] adapters receive the single blanket
/// implementation, while the framework's admitted multi-provider router has
/// one internal implementation. Neither can override binding equivalence or
/// construct an admitted-operation token.
///
/// External code cannot replace the admission sequence:
///
/// ```compile_fail
/// use cymule_runtime::BoundPluginHost;
/// struct ForgedHost;
/// impl BoundPluginHost for ForgedHost {}
/// ```
pub trait BoundPluginHost: bound_plugin_host::Sealed {
    /// Verify that this exact bound host realizes one complete immutable
    /// execution binding.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when the binding is invalid or the bound host
    /// does not realize its exact provider selections.
    fn admit_execution_binding(&mut self, binding: &ExecutionBinding) -> RuntimeResult<()>
    where
        Self: Sized,
    {
        bound_plugin_host::Sealed::framework_admit_execution_binding(self, binding)
    }

    /// Admit one exact historical operation against the runtime owner's current
    /// executable pin. This binding-equivalence step is framework-owned and
    /// cannot be replaced by a provider adapter.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when live manifest lookup or internal routed
    /// provider admission fails. Binding mismatch returns an unavailable token.
    fn admit_bound_operation<'a>(
        &'a mut self,
        current_binding: &ExecutionBinding,
        historical_binding: &ExecutionBinding,
        kind: ExecutionOperationKind,
        operation: &str,
    ) -> RuntimeResult<BoundOperationAdmission<'a>>
    where
        Self: Sized,
    {
        if let Err(error) = current_binding.verify_selected_operation_equivalence(
            historical_binding,
            kind,
            operation,
        ) {
            return Ok(BoundOperationAdmission::unavailable(kind, operation, error));
        }
        bound_plugin_host::Sealed::framework_admit_equivalent_operation(
            self,
            current_binding,
            historical_binding,
            kind,
            operation,
        )
    }

    /// Invoke under one exact historical execution binding. The runtime owner
    /// supplies its already-admitted current binding so direct hosts and
    /// routers share one selected-operation equivalence authority.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when request admission, live provider admission,
    /// invocation, or response verification fails.
    fn invoke_bound(
        &mut self,
        current_binding: &ExecutionBinding,
        historical_binding: &ExecutionBinding,
        request: PluginRequest,
    ) -> RuntimeResult<PluginResponse>
    where
        Self: Sized,
    {
        let (kind, operation) = request_operation(&request)?;
        let admission =
            self.admit_bound_operation(current_binding, historical_binding, kind, operation)?;
        admission.invoke(request)
    }
}

impl<T: bound_plugin_host::Sealed> BoundPluginHost for T {}

/// Runtime router that dispatches each operation to the exact provider selected
/// by an admitted [`ExecutionBinding`].
///
/// Child manifests are capability advertisements only. The immutable binding
/// remains the routing authority: an advertised but unbound operation is never
/// callable, and a bound operation must be advertised by its selected provider
/// with the exact implementation and operation revision.
///
/// The router deliberately has no raw [`PluginHost`] implementation:
///
/// ```compile_fail
/// use cymule_runtime::{AdmittedPluginRouter, PluginHost};
/// fn require_raw_host<T: PluginHost>() {}
/// require_raw_host::<AdmittedPluginRouter>();
/// ```
pub struct AdmittedPluginRouter {
    binding: ExecutionBinding,
    providers: BTreeMap<String, Box<dyn PluginHost>>,
}

impl AdmittedPluginRouter {
    /// Verify every selected provider against its live capability advertisement.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when the binding is invalid, a selected provider
    /// is absent, Describe fails, or a live manifest differs from its selection.
    pub fn new(
        binding: ExecutionBinding,
        mut providers: BTreeMap<String, Box<dyn PluginHost>>,
    ) -> RuntimeResult<Self> {
        binding.verify()?;
        for provider_id in binding.required_provider_ids() {
            let provider = providers.get_mut(&provider_id).ok_or_else(|| {
                RuntimeError::from(crate::CompositionError::InvalidExecutionBinding {
                    reason: format!("execution binding selected missing provider {provider_id}"),
                })
            })?;
            let manifest = provider.describe()?;
            binding.verify_provider_manifest(&provider_id, &manifest)?;
        }
        Ok(Self { binding, providers })
    }
}

impl bound_plugin_host::Sealed for AdmittedPluginRouter {
    fn framework_admit_execution_binding(
        &mut self,
        binding: &ExecutionBinding,
    ) -> RuntimeResult<()> {
        binding.verify()?;
        if binding != &self.binding {
            return Err(RuntimeError::from(
                crate::CompositionError::InvalidExecutionBinding {
                    reason: "runtime binding does not match the router authority".to_owned(),
                },
            ));
        }
        for provider_id in binding.required_provider_ids() {
            let provider = self.providers.get_mut(&provider_id).ok_or_else(|| {
                RuntimeError::from(crate::CompositionError::InvalidExecutionBinding {
                    reason: format!("execution binding selected missing provider {provider_id}"),
                })
            })?;
            let manifest = provider.describe()?;
            binding
                .verify_provider_manifest(&provider_id, &manifest)
                .map_err(RuntimeError::from)?;
        }
        Ok(())
    }

    fn framework_admit_equivalent_operation<'a>(
        &'a mut self,
        current_binding: &ExecutionBinding,
        historical_binding: &ExecutionBinding,
        kind: ExecutionOperationKind,
        operation: &str,
    ) -> RuntimeResult<BoundOperationAdmission<'a>> {
        if current_binding != &self.binding {
            return Ok(BoundOperationAdmission::unavailable(
                kind,
                operation,
                crate::CompositionError::InvalidExecutionBinding {
                    reason: "runtime binding does not match the router authority".to_owned(),
                },
            ));
        }
        let selected = match kind {
            ExecutionOperationKind::Component => historical_binding.components.get(operation),
            ExecutionOperationKind::Effect => historical_binding.effects.get(operation),
        }
        .ok_or_else(|| {
            RuntimeError::from(crate::CompositionError::MissingOperationBinding {
                kind,
                operation: operation.to_owned(),
            })
        })?;
        let provider = self
            .providers
            .get_mut(&selected.provider_id)
            .ok_or_else(|| {
                RuntimeError::from(crate::CompositionError::InvalidExecutionBinding {
                    reason: format!(
                        "execution binding selected missing provider {}",
                        selected.provider_id
                    ),
                })
            })?;
        let manifest = provider.describe()?;
        match historical_binding.verify_operation_manifest(kind, operation, &manifest) {
            Ok(()) => Ok(BoundOperationAdmission::admitted(
                kind,
                operation,
                move |request| provider.invoke(request),
            )),
            Err(error) => Ok(BoundOperationAdmission::unavailable(kind, operation, error)),
        }
    }
}

fn request_operation(request: &PluginRequest) -> RuntimeResult<(ExecutionOperationKind, &str)> {
    match request {
        PluginRequest::Describe => Err(RuntimeError::plugin_defect(
            "a bound invocation cannot describe aggregate capability",
        )),
        PluginRequest::Call { component, .. } => Ok((ExecutionOperationKind::Component, component)),
        PluginRequest::PrepareEffect { operation, .. }
        | PluginRequest::DispatchEffect { operation, .. }
        | PluginRequest::ReconcileEffect { operation, .. } => {
            Ok((ExecutionOperationKind::Effect, operation))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_failure_and_defect_share_the_unicode_scalar_message_bound() {
        let expected = PluginExpectedFailure {
            code: "evaluation_rejected".to_owned(),
            message: "🧭".repeat(2000),
        };
        expected
            .verify()
            .expect("2000 Unicode scalars are admitted regardless of UTF-8 length");
        PluginResponse::Defect {
            code: "provider_unavailable".to_owned(),
            message: expected.message.clone(),
        }
        .verify()
        .expect("Defect uses the same exact message domain");

        for message in [
            String::new(),
            "🧭".repeat(2001),
            "invalid\nmessage".to_owned(),
        ] {
            assert!(
                PluginExpectedFailure {
                    code: "evaluation_rejected".to_owned(),
                    message: message.clone(),
                }
                .verify()
                .is_err()
            );
            assert!(
                PluginResponse::Defect {
                    code: "provider_unavailable".to_owned(),
                    message,
                }
                .verify()
                .is_err()
            );
        }
    }

    #[test]
    fn manifest_capability_maps_are_required_on_the_wire() {
        for missing in ["components", "effects"] {
            let mut manifest = serde_json::json!({
                "plugin_version": PLUGIN_VERSION,
                "implementation_id": "test.required-manifest",
                "components": {},
                "effects": {},
            });
            manifest
                .as_object_mut()
                .expect("manifest is an object")
                .remove(missing);
            assert!(serde_json::from_value::<PluginManifest>(manifest).is_err());
        }
    }

    #[test]
    fn plugin_wire_uses_one_strict_operation_and_effect_intent_domain() {
        let valid_intent =
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        PluginRequest::PrepareEffect {
            operation: "test.capture".to_owned(),
            intent_id: valid_intent.to_owned(),
            input: Value::Null,
        }
        .verify()
        .expect("content-addressed Effect request validates");

        for intent_id in [
            "intent:not-content-addressed",
            "sha256:short",
            "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ] {
            assert!(
                PluginRequest::PrepareEffect {
                    operation: "test.capture".to_owned(),
                    intent_id: intent_id.to_owned(),
                    input: Value::Null,
                }
                .verify()
                .is_err()
            );
        }

        for operation in ["", "has space", "snowman.☃", &"x".repeat(161)] {
            let manifest = PluginManifest {
                plugin_version: PLUGIN_VERSION.to_owned(),
                implementation_id: "test.provider".to_owned(),
                components: BTreeMap::from([(
                    operation.to_owned(),
                    PluginOperation {
                        implementation_revision: "revision-1".to_owned(),
                    },
                )]),
                effects: BTreeMap::new(),
            };
            assert!(manifest.verify().is_err());
        }
    }

    #[test]
    fn plugin_message_and_manifest_bounds_are_exact_and_precede_typed_authority() {
        let mut exact_request = br#"{"type":"describe"}"#.to_vec();
        exact_request.resize(MAX_PLUGIN_MESSAGE_BYTES, b' ');
        assert_eq!(
            decode_plugin_request(&exact_request).expect("exact raw plugin bound decodes"),
            PluginRequest::Describe
        );
        exact_request.push(b' ');
        assert!(matches!(
            decode_plugin_request(&exact_request),
            Err(RuntimeError::PluginDefect { code, .. }) if code == "plugin_message_too_large"
        ));

        let empty = PluginResponse::CallResult {
            value: Value::String(String::new()),
        };
        let overhead = canonical_bytes(&empty).unwrap().len();
        let exact_response = PluginResponse::CallResult {
            value: Value::String("x".repeat(MAX_PLUGIN_MESSAGE_BYTES - overhead)),
        };
        exact_response
            .verify()
            .expect("exact canonical plugin response bound verifies");
        let mut over_response = exact_response;
        let PluginResponse::CallResult { value } = &mut over_response else {
            unreachable!()
        };
        if let Value::String(value) = value {
            value.push('x');
        } else {
            unreachable!()
        }
        assert!(matches!(
            over_response.verify(),
            Err(RuntimeError::PluginDefect { code, .. }) if code == "plugin_message_too_large"
        ));

        let components = (0..MAX_EXECUTION_OPERATIONS_PER_KIND)
            .map(|index| {
                (
                    format!("component.{index:04}"),
                    PluginOperation {
                        implementation_revision: "revision-1".to_owned(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let manifest = PluginManifest {
            plugin_version: PLUGIN_VERSION.to_owned(),
            implementation_id: "test.bounded-manifest".to_owned(),
            components,
            effects: BTreeMap::new(),
        };
        manifest
            .verify()
            .expect("exact manifest operation bound verifies");
        let mut over_manifest = serde_json::to_value(&manifest).unwrap();
        over_manifest["components"].as_object_mut().unwrap().insert(
            "component.over".to_owned(),
            serde_json::to_value(PluginOperation {
                implementation_revision: "revision-1".to_owned(),
            })
            .unwrap(),
        );
        let bytes = canonical_bytes(&over_manifest).unwrap();
        let raw_manifest: Result<PluginManifest, _> = cymule_core::decode_json(&bytes);
        assert!(raw_manifest.is_err());
    }
}
