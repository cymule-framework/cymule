use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    EvolutionError, EvolutionResult, MigrationAdapterDescriptor, MigrationAdapterRequest,
    MigrationOutput, MigrationRequest, ShadowDriverDescriptor, ShadowOutput, ShadowRequest,
};

use cymule_core::ArtifactRef;
use cymule_durable_protocol::Continuation;

/// Frozen process protocol shared by migration and shadow implementations.
pub const EVOLUTION_PLUGIN_PROTOCOL_VERSION: &str =
    cymule_runtime::EVOLUTION_PLUGIN_PROTOCOL_VERSION;
/// Hard raw JSON bound for either direction of one process-provider exchange.
pub const MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES: usize =
    cymule_runtime::EVOLUTION_PLUGIN_MESSAGE_LIMIT;

/// Closed request envelope for one process-hosted evolution operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionPluginRequestEnvelope {
    /// Exact process protocol generation.
    pub evolution_plugin_protocol: String,
    /// Immutable revision of the sealed process closure.
    pub implementation_revision: String,
    /// Closed operation request.
    pub request: EvolutionPluginRequest,
}

impl EvolutionPluginRequestEnvelope {
    /// Construct one current-generation request.
    pub fn new(
        implementation_revision: impl Into<String>,
        request: EvolutionPluginRequest,
    ) -> Self {
        Self {
            evolution_plugin_protocol: EVOLUTION_PLUGIN_PROTOCOL_VERSION.to_owned(),
            implementation_revision: implementation_revision.into(),
            request,
        }
    }

    /// Verify envelope-local protocol and executable-revision authority without
    /// duplicating the potentially large Continuation payload.
    ///
    /// # Errors
    ///
    /// Returns validation failure for an unsupported generation or malformed
    /// immutable implementation revision.
    pub fn into_verified(mut self) -> EvolutionResult<Self> {
        if self.evolution_plugin_protocol != EVOLUTION_PLUGIN_PROTOCOL_VERSION {
            return Err(EvolutionError::Validation(format!(
                "unsupported evolution plugin protocol {:?}",
                self.evolution_plugin_protocol
            )));
        }
        cymule_core::validate_content_id(
            "evolution plugin implementation revision",
            &self.implementation_revision,
        )
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
        self.request = self.request.into_verified()?;
        match &self.request {
            EvolutionPluginRequest::Migrate { request }
                if request.intent.adapter_revision != self.implementation_revision =>
            {
                return Err(EvolutionError::Conflict(
                    "migration process revision does not match the semantic adapter revision"
                        .to_owned(),
                ));
            }
            EvolutionPluginRequest::ExecuteShadow { request }
                if request.driver_revision != self.implementation_revision =>
            {
                return Err(EvolutionError::Conflict(
                    "shadow process revision does not match the semantic driver revision"
                        .to_owned(),
                ));
            }
            _ => {}
        }
        Ok(self)
    }
}

/// Closed serializable projection of the non-serializable provider request
/// assembled by Durable. This type is process egress, never an M4 command or
/// persistence input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionPluginMigrationRequest {
    /// Public semantic migration intent.
    pub intent: MigrationRequest,
    /// Content-derived exact source witness identity.
    pub source_witness_id: String,
    /// Complete exact source Continuation.
    pub source_continuation: Continuation,
    /// Immutable source-state Artifact selected from the Continuation.
    pub input_state: ArtifactRef,
    /// Exact source `ExecutionBinding` Artifact.
    pub source_binding: ArtifactRef,
    /// Exact target `ExecutionBinding` Artifact.
    pub target_binding: ArtifactRef,
}

impl EvolutionPluginMigrationRequest {
    /// Project one verified in-process request onto the closed process wire.
    pub fn from_adapter_request(request: &MigrationAdapterRequest) -> Self {
        Self {
            intent: request.intent.clone(),
            source_witness_id: request.source_witness_id.clone(),
            source_continuation: request.source_continuation.clone(),
            input_state: request.input_state.clone(),
            source_binding: request.source_binding.clone(),
            target_binding: request.target_binding.clone(),
        }
    }

    /// Reconstruct and verify the provider request received by the process.
    ///
    /// # Errors
    ///
    /// Returns an error when any wire field is malformed or the exact source
    /// values do not form one quiescent migration authority.
    pub fn into_adapter_request(self) -> EvolutionResult<MigrationAdapterRequest> {
        let request = MigrationAdapterRequest {
            intent: self.intent,
            source_witness_id: self.source_witness_id,
            source_continuation: self.source_continuation,
            input_state: self.input_state,
            source_binding: self.source_binding,
            target_binding: self.target_binding,
        };
        request.verify()?;
        Ok(request)
    }

    fn into_verified(self) -> EvolutionResult<Self> {
        let request = self.into_adapter_request()?;
        Ok(Self {
            intent: request.intent,
            source_witness_id: request.source_witness_id,
            source_continuation: request.source_continuation,
            input_state: request.input_state,
            source_binding: request.source_binding,
            target_binding: request.target_binding,
        })
    }
}

/// Closed process-hosted evolution request union.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvolutionPluginRequest {
    /// Describe one exact migration implementation.
    DescribeMigration {},
    /// Execute one exact checked migration.
    Migrate {
        /// Complete migration request.
        request: Box<EvolutionPluginMigrationRequest>,
    },
    /// Describe isolated shadow execution behavior.
    DescribeShadow {},
    /// Execute one non-authoritative shadow comparison.
    ExecuteShadow {
        /// Complete shadow request.
        request: Box<ShadowRequest>,
    },
}

impl EvolutionPluginRequest {
    fn into_verified(self) -> EvolutionResult<Self> {
        match self {
            Self::DescribeMigration {} => Ok(Self::DescribeMigration {}),
            Self::DescribeShadow {} => Ok(Self::DescribeShadow {}),
            Self::Migrate { request } => Ok(Self::Migrate {
                request: Box::new((*request).into_verified()?),
            }),
            Self::ExecuteShadow { request } => {
                request.verify()?;
                Ok(Self::ExecuteShadow { request })
            }
        }
    }
}

/// Closed response envelope for one process-hosted evolution operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvolutionPluginResponseEnvelope {
    /// The requested operation returned a typed result.
    Success {
        /// Exact process protocol generation.
        evolution_plugin_protocol: String,
        /// Closed success value.
        response: Box<EvolutionPluginResponse>,
    },
    /// The provider could not execute the requested operation.
    Failure {
        /// Exact process protocol generation.
        evolution_plugin_protocol: String,
        /// Closed provider failure.
        error: EvolutionPluginFailure,
    },
}

impl EvolutionPluginResponseEnvelope {
    /// Construct one current-generation success.
    pub fn success(response: EvolutionPluginResponse) -> Self {
        Self::Success {
            evolution_plugin_protocol: EVOLUTION_PLUGIN_PROTOCOL_VERSION.to_owned(),
            response: Box::new(response),
        }
    }

    /// Construct one current-generation failure.
    pub fn failure(error: EvolutionPluginFailure) -> Self {
        Self::Failure {
            evolution_plugin_protocol: EVOLUTION_PLUGIN_PROTOCOL_VERSION.to_owned(),
            error,
        }
    }

    /// Verify the version and failure bounds, returning the typed success or
    /// the provider's closed substrate failure.
    ///
    /// # Errors
    ///
    /// Returns validation for a wrong protocol generation, a plugin defect for
    /// a malformed failure object, or the provider's closed substrate failure.
    pub fn into_result(self) -> EvolutionResult<EvolutionPluginResponse> {
        match self {
            Self::Success {
                evolution_plugin_protocol,
                response,
            } => {
                verify_protocol(&evolution_plugin_protocol)?;
                response.verify_product_bounds()?;
                Ok(*response)
            }
            Self::Failure {
                evolution_plugin_protocol,
                error,
            } => {
                verify_protocol(&evolution_plugin_protocol)?;
                error.verify()?;
                Err(error.into_evolution_error())
            }
        }
    }

    fn into_verified(self) -> EvolutionResult<Self> {
        match &self {
            Self::Success {
                evolution_plugin_protocol,
                response,
            } => {
                verify_protocol(evolution_plugin_protocol)?;
                response.verify_product_bounds()?;
            }
            Self::Failure {
                evolution_plugin_protocol,
                error,
            } => {
                verify_protocol(evolution_plugin_protocol)?;
                error.verify()?;
            }
        }
        Ok(self)
    }
}

/// Closed success values for the evolution process protocol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvolutionPluginResponse {
    /// Migration implementation descriptor.
    MigrationDescriptor {
        /// Pinned migration contract.
        descriptor: MigrationAdapterDescriptor,
    },
    /// Complete migrated Continuation and Artifact product.
    Migrated {
        /// Migration output.
        output: Box<MigrationOutput>,
    },
    /// Shadow implementation descriptor.
    ShadowDescriptor {
        /// Pinned shadow contract.
        descriptor: ShadowDriverDescriptor,
    },
    /// Complete shadow comparison output.
    ShadowExecuted {
        /// Shadow output.
        output: ShadowOutput,
    },
}

impl EvolutionPluginResponse {
    fn verify_product_bounds(&self) -> EvolutionResult<()> {
        match self {
            Self::Migrated { output } => {
                output.continuation.verify_wire().map_err(|error| {
                    EvolutionError::PluginDefect {
                        code: MigrationOutput::INVALID_OUTPUT_DEFECT_CODE.to_owned(),
                        message: format!(
                            "migration plugin returned an invalid bounded Continuation: {error}"
                        ),
                    }
                })?;
                output.verify_artifact_limits()
            }
            Self::ShadowExecuted { output } => output.verify_evidence_limits(),
            Self::MigrationDescriptor { .. } | Self::ShadowDescriptor { .. } => Ok(()),
        }
    }
}

/// Closed provider failure carried by the evolution process protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "category", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvolutionPluginFailure {
    /// Provider cancellation before an ambiguous dispatch.
    Cancelled {
        /// Stable lowercase provider code.
        code: String,
        /// Human-readable bounded summary.
        message: String,
    },
    /// Provider deadline expiration before an ambiguous dispatch.
    TimedOut {
        /// Stable lowercase provider code.
        code: String,
        /// Human-readable bounded summary.
        message: String,
    },
    /// Structured executable contract rejection.
    Contract {
        /// Exact contract violation, including bounded issue paths.
        violation: cymule_runtime::ContractViolation,
    },
    /// Provider binding, identity, or causal integrity violation.
    Integrity {
        /// Stable lowercase provider code.
        code: String,
        /// Human-readable bounded summary.
        message: String,
    },
    /// Provider returned a malformed closed response.
    PluginDefect {
        /// Stable lowercase defect code.
        code: String,
        /// Human-readable bounded summary.
        message: String,
    },
    /// Provider substrate failed before producing a semantic result.
    Substrate {
        /// Stable lowercase substrate code.
        code: String,
        /// Human-readable bounded summary.
        message: String,
    },
}

impl EvolutionPluginFailure {
    /// Verify the exact schema-aligned failure domain.
    ///
    /// # Errors
    ///
    /// Returns a plugin defect when the code or message is outside the frozen
    /// schema domain.
    pub fn verify(&self) -> EvolutionResult<()> {
        match self {
            Self::Cancelled { code, message }
            | Self::TimedOut { code, message }
            | Self::Integrity { code, message }
            | Self::PluginDefect { code, message }
            | Self::Substrate { code, message } => verify_failure_fields(code, message),
            Self::Contract { violation } => verify_contract_violation(violation),
        }
    }

    fn into_evolution_error(self) -> EvolutionError {
        match self {
            Self::Cancelled { code, message } => EvolutionError::Cancelled { code, message },
            Self::TimedOut { code, message } => EvolutionError::TimedOut { code, message },
            Self::Contract { violation } => EvolutionError::Contract(violation),
            Self::Integrity { code, message } => EvolutionError::Integrity { code, message },
            Self::PluginDefect { code, message } => EvolutionError::PluginDefect { code, message },
            Self::Substrate { code, message } => EvolutionError::Substrate { code, message },
        }
    }
}

fn invalid_failure() -> EvolutionError {
    EvolutionError::PluginDefect {
        code: "invalid_evolution_plugin_failure".to_owned(),
        message: "evolution plugin returned an invalid bounded failure object".to_owned(),
    }
}

fn verify_failure_fields(code: &str, message: &str) -> EvolutionResult<()> {
    if cymule_core::validate_failure_code(code).is_err()
        || !(1..=2000).contains(&message.chars().count())
    {
        return Err(invalid_failure());
    }
    Ok(())
}

fn verify_contract_violation(violation: &cymule_runtime::ContractViolation) -> EvolutionResult<()> {
    violation.verify().map_err(|_| invalid_failure())
}

/// Decode one closed request through the shared duplicate-rejecting, safe-
/// number normalizing strict JSON ingress.
///
/// # Errors
///
/// Returns validation failure for malformed JSON, duplicate or unknown fields,
/// unsafe numbers, missing members, or an unsupported protocol generation.
pub fn decode_evolution_plugin_request(
    input: &[u8],
) -> EvolutionResult<EvolutionPluginRequestEnvelope> {
    decode_closed_message::<EvolutionPluginRequestEnvelope>(input)?.into_verified()
}

/// Decode one closed response through the same bounded duplicate-rejecting,
/// safe-number-normalizing strict JSON ingress as requests.
///
/// # Errors
///
/// Returns validation failure for an oversized or malformed envelope and a
/// plugin defect for an invalid bounded failure body.
pub fn decode_evolution_plugin_response(
    input: &[u8],
) -> EvolutionResult<EvolutionPluginResponseEnvelope> {
    decode_closed_message::<EvolutionPluginResponseEnvelope>(input)?.into_verified()
}

fn decode_closed_message<T>(input: &[u8]) -> EvolutionResult<T>
where
    T: DeserializeOwned + Serialize,
{
    if input.len() > MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES {
        return Err(EvolutionError::Validation(format!(
            "evolution plugin message uses {} raw bytes, above the {MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES} byte bound",
            input.len()
        )));
    }
    let raw =
        cymule_runtime::decode_strict_json_value(input).map_err(EvolutionError::Validation)?;
    let message: T = serde_json::from_value(raw.clone())
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    let normalized = serde_json::to_value(&message)
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    cymule_runtime::validate_json_typed_roundtrip_bytes(input, &raw, &normalized)
        .map_err(EvolutionError::Validation)?;
    Ok(message)
}

fn verify_protocol(version: &str) -> EvolutionResult<()> {
    if version != EVOLUTION_PLUGIN_PROTOCOL_VERSION {
        return Err(EvolutionError::Validation(format!(
            "unsupported evolution plugin protocol {version:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use cymule_core::{
        ArtifactRecord, InvocationPathSegment, MAX_EXACT_INTEGER, ROOT_SCOPE_ID, artifact_ref,
        canonical_bytes,
    };
    use cymule_durable_protocol::{
        CONTINUATION_STATE_VERSION, ContinuationStatus, FrameState,
        MAX_CONTINUATION_AGGREGATE_ITEMS, MAX_CONTINUATION_IDENTITY_SCALARS,
        MAX_CONTINUATION_WIRE_BYTES, MAX_FRAME_INVOCATION_DEPTH, MAX_REGION_PATH_DEPTH,
    };

    use super::*;

    const REVISION: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";

    #[derive(serde::Serialize)]
    struct MigrationArtifactProducts<'a> {
        artifacts: &'a [ArtifactRecord],
        evidence: &'a ArtifactRecord,
    }

    fn zero_artifact(kind: &str, payload_len: usize) -> ArtifactRecord {
        let bytes = vec![0; payload_len];
        ArtifactRecord {
            reference: artifact_ref(kind, &bytes).expect("valid boundary Artifact reference"),
            bytes,
        }
    }

    fn maximum_zero_artifact(
        kind: &str,
        maximum: usize,
        encoded_len: impl Fn(&ArtifactRecord) -> usize,
    ) -> ArtifactRecord {
        let empty = zero_artifact(kind, 0);
        let empty_len = encoded_len(&empty);
        // Artifact bytes use padded Base64: N raw bytes add
        // `4 * ceil(N / 3)` canonical string bytes relative to the empty value.
        let payload_len = 3 * ((maximum - empty_len) / 4);
        let record = zero_artifact(kind, payload_len);
        assert!(encoded_len(&record) <= maximum);
        assert!(encoded_len(&zero_artifact(kind, payload_len + 1)) > maximum);
        record
    }

    fn maximum_migration_evidence() -> ArtifactRecord {
        maximum_zero_artifact(
            "cymule.test-migration-evidence/1",
            MigrationOutput::MAX_ARTIFACT_CANONICAL_BYTES,
            |evidence| {
                canonical_bytes(&MigrationArtifactProducts {
                    artifacts: &[],
                    evidence,
                })
                .unwrap()
                .len()
            },
        )
    }

    fn maximum_shadow_evidence() -> ArtifactRecord {
        maximum_zero_artifact(
            "cymule.test-shadow-evidence/1",
            ShadowOutput::MAX_EVIDENCE_CANONICAL_BYTES,
            |evidence| canonical_bytes(evidence).unwrap().len(),
        )
    }

    fn dense_bounded_continuation() -> Continuation {
        let input = artifact_ref("cymule.test-migration-input/1", b"input")
            .expect("valid dense Continuation input");
        let mut remaining_region_items =
            MAX_CONTINUATION_AGGREGATE_ITEMS - 1 - 1 - MAX_FRAME_INVOCATION_DEPTH;
        let exact_index = usize::try_from(MAX_EXACT_INTEGER).expect("64-bit test platform");
        let mut invocation_path = Vec::with_capacity(MAX_FRAME_INVOCATION_DEPTH);
        for _ in 0..MAX_FRAME_INVOCATION_DEPTH {
            let region_depth = remaining_region_items.min(MAX_REGION_PATH_DEPTH);
            remaining_region_items -= region_depth;
            invocation_path.push(InvocationPathSegment {
                site_id: "s".to_owned(),
                region_path: vec![exact_index; region_depth],
                scope_id: "q".to_owned(),
            });
        }
        assert_eq!(remaining_region_items, 0);
        let mut continuation = Continuation {
            continuation_version: CONTINUATION_STATE_VERSION.to_owned(),
            run_id: "run-maximum-process-product".to_owned(),
            plan_id: format!("sha256:{}", "5".repeat(64)),
            binding_context: input.artifact_id.clone(),
            frames: vec![FrameState {
                definition_id: "definition-maximum".to_owned(),
                invocation_id: "invocation-maximum".to_owned(),
                invocation_path,
                scope_id: ROOT_SCOPE_ID.to_owned(),
                input,
                region_path: Vec::new(),
                next_step: 0,
                locals: BTreeMap::new(),
            }],
            state: None,
            wait_set: BTreeSet::new(),
            scope_stack: vec![ROOT_SCOPE_ID.to_owned()],
            epoch: 1,
            execution_fence: 1,
            execution_claim: None,
            status: ContinuationStatus::Ready,
        };
        let mut identity_scalars = {
            let frame = continuation.frames.first().unwrap();
            continuation.continuation_version.chars().count()
                + continuation.run_id.chars().count()
                + continuation.plan_id.chars().count()
                + continuation.binding_context.chars().count()
                + continuation
                    .scope_stack
                    .iter()
                    .map(|scope| scope.chars().count())
                    .sum::<usize>()
                + frame.definition_id.chars().count()
                + frame.invocation_id.chars().count()
                + frame.scope_id.chars().count()
                + frame.input.identity_version.chars().count()
                + frame.input.artifact_id.chars().count()
                + frame.input.kind.chars().count()
                + frame
                    .invocation_path
                    .iter()
                    .map(|segment| {
                        segment.site_id.chars().count() + segment.scope_id.chars().count()
                    })
                    .sum::<usize>()
        };
        let mut remaining_scalars = MAX_CONTINUATION_IDENTITY_SCALARS - identity_scalars;
        let frame = continuation.frames.first_mut().unwrap();
        for segment in &mut frame.invocation_path {
            for identity in [&mut segment.site_id, &mut segment.scope_id] {
                let added = remaining_scalars.min(512 - identity.chars().count());
                identity.push_str(&"🦀".repeat(added));
                remaining_scalars -= added;
                identity_scalars += added;
            }
        }
        assert_eq!(remaining_scalars, 0);
        assert_eq!(identity_scalars, MAX_CONTINUATION_IDENTITY_SCALARS);
        continuation
            .verify_wire()
            .expect("dense Continuation remains inside every closed bound");
        let encoded = serde_json::to_vec(&continuation).unwrap();
        assert!(encoded.len() <= MAX_CONTINUATION_WIRE_BYTES);
        assert!(encoded.len() > MAX_CONTINUATION_WIRE_BYTES / 4);
        continuation
    }

    #[test]
    fn request_decoder_is_closed_strict_and_versioned() {
        let valid = format!(
            r#"{{"evolution_plugin_protocol":"{EVOLUTION_PLUGIN_PROTOCOL_VERSION}","implementation_revision":"{REVISION}","request":{{"type":"describe_migration"}}}}"#
        );
        decode_evolution_plugin_request(valid.as_bytes()).expect("closed request decodes");

        for invalid in [
            format!(
                r#"{{"evolution_plugin_protocol":"{EVOLUTION_PLUGIN_PROTOCOL_VERSION}","implementation_revision":"{REVISION}","implementation_revision":"{REVISION}","request":{{"type":"describe_migration"}}}}"#
            ),
            format!(
                r#"{{"evolution_plugin_protocol":"cymule.evolution-plugin/1","implementation_revision":"{REVISION}","request":{{"type":"describe_migration"}}}}"#
            ),
            format!(
                r#"{{"implementation_revision":"{REVISION}","request":{{"type":"describe_migration"}}}}"#
            ),
            format!(
                r#"{{"evolution_plugin_protocol":"{EVOLUTION_PLUGIN_PROTOCOL_VERSION}","implementation_revision":"{REVISION}","request":{{"type":"describe_migration","unsafe":9007199254740992}}}}"#
            ),
            format!(
                r#"{{"evolution_plugin_protocol":"{EVOLUTION_PLUGIN_PROTOCOL_VERSION}","implementation_revision":"{REVISION}","request":{{"type":"describe_migration","unknown":true}}}}"#
            ),
        ] {
            assert!(
                decode_evolution_plugin_request(invalid.as_bytes()).is_err(),
                "malformed request was admitted: {invalid}"
            );
        }

        let mut exact = valid.into_bytes();
        exact.resize(MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES, b' ');
        decode_evolution_plugin_request(&exact).expect("exact raw request bound is accepted");
        exact.push(b' ');
        assert!(decode_evolution_plugin_request(&exact).is_err());
    }

    #[test]
    fn request_revision_must_equal_the_selected_semantic_provider_revision() {
        let input = cymule_core::artifact_ref("cymule.test-shadow-input/1", b"input").unwrap();
        let request = EvolutionPluginRequestEnvelope::new(
            REVISION,
            EvolutionPluginRequest::ExecuteShadow {
                request: Box::new(ShadowRequest {
                    comparison_id: "comparison-1".to_owned(),
                    decision_id: "decision-1".to_owned(),
                    subject: "subject-1".to_owned(),
                    primary_plan: format!("sha256:{}", "2".repeat(64)),
                    shadow_plan: format!("sha256:{}", "3".repeat(64)),
                    input,
                    driver_id: "shadow-main".to_owned(),
                    driver_revision: format!("sha256:{}", "4".repeat(64)),
                    comparison_policy: "exact-output/1".to_owned(),
                }),
            },
        );
        let encoded = serde_json::to_vec(&request).unwrap();
        assert!(matches!(
            decode_evolution_plugin_request(&encoded),
            Err(EvolutionError::Conflict(message))
                if message.contains("semantic driver revision")
        ));
    }

    #[test]
    fn failure_bounds_are_unicode_scalar_aligned() {
        EvolutionPluginFailure::Substrate {
            code: "provider_unavailable_2".to_owned(),
            message: "🧭".repeat(2000),
        }
        .verify()
        .expect("schema boundary is accepted");
        for failure in [
            EvolutionPluginFailure::Substrate {
                code: "_provider".to_owned(),
                message: "invalid code".to_owned(),
            },
            EvolutionPluginFailure::Substrate {
                code: "provider".to_owned(),
                message: "🧭".repeat(2001),
            },
        ] {
            assert!(failure.verify().is_err());
        }
    }

    fn round_trip_failure(failure: EvolutionPluginFailure) -> EvolutionError {
        let encoded = serde_json::to_vec(&EvolutionPluginResponseEnvelope::failure(failure))
            .expect("failure envelope encodes");
        decode_evolution_plugin_response(&encoded)
            .expect("failure envelope decodes strictly")
            .into_result()
            .expect_err("failure envelope cannot become success")
    }

    #[test]
    fn failure_categories_round_trip_without_losing_code_or_structure() {
        assert!(matches!(
            round_trip_failure(EvolutionPluginFailure::Cancelled {
                code: "provider_cancelled".to_owned(),
                message: "cancelled before dispatch".to_owned(),
            }),
            EvolutionError::Cancelled { code, message }
                if code == "provider_cancelled" && message == "cancelled before dispatch"
        ));
        assert!(matches!(
            round_trip_failure(EvolutionPluginFailure::TimedOut {
                code: "provider_deadline".to_owned(),
                message: "deadline elapsed".to_owned(),
            }),
            EvolutionError::TimedOut { code, message }
                if code == "provider_deadline" && message == "deadline elapsed"
        ));
        assert!(matches!(
            round_trip_failure(EvolutionPluginFailure::Integrity {
                code: "binding_changed".to_owned(),
                message: "provider binding changed".to_owned(),
            }),
            EvolutionError::Integrity { code, message }
                if code == "binding_changed" && message == "provider binding changed"
        ));
        assert!(matches!(
            round_trip_failure(EvolutionPluginFailure::PluginDefect {
                code: "invalid_provider_output".to_owned(),
                message: "provider returned the wrong variant".to_owned(),
            }),
            EvolutionError::PluginDefect { code, message }
                if code == "invalid_provider_output"
                    && message == "provider returned the wrong variant"
        ));
        assert!(matches!(
            round_trip_failure(EvolutionPluginFailure::Substrate {
                code: "provider_unavailable".to_owned(),
                message: "provider is unavailable".to_owned(),
            }),
            EvolutionError::Substrate { code, message }
                if code == "provider_unavailable" && message == "provider is unavailable"
        ));

        let violation = cymule_runtime::ContractViolation {
            phase: cymule_runtime::ContractPhase::Execution,
            target: cymule_runtime::ContractTarget {
                boundary: cymule_runtime::ContractBoundary::Definition,
                id: "migrate".to_owned(),
                side: cymule_runtime::ContractSide::Output,
            },
            issues: vec![cymule_runtime::ContractIssue {
                kind: cymule_runtime::ContractIssueKind::Validation,
                instance_path: "/state".to_owned(),
                schema_path: "/required".to_owned(),
                message: "required state is missing".to_owned(),
            }],
        };
        assert!(matches!(
            round_trip_failure(EvolutionPluginFailure::Contract {
                violation: violation.clone(),
            }),
            EvolutionError::Contract(actual) if actual == violation
        ));

        let mut missing_kind = serde_json::to_value(EvolutionPluginResponseEnvelope::failure(
            EvolutionPluginFailure::Contract {
                violation: violation.clone(),
            },
        ))
        .unwrap();
        missing_kind["error"]["violation"]["issues"][0]
            .as_object_mut()
            .unwrap()
            .remove("kind");
        let missing_kind = serde_json::to_vec(&missing_kind).unwrap();
        assert!(decode_evolution_plugin_response(&missing_kind).is_err());

        let mut duplicate_issue = violation;
        let repeated = duplicate_issue.issues[0].clone();
        duplicate_issue.issues.push(repeated);
        let duplicate_issue = serde_json::to_vec(&EvolutionPluginResponseEnvelope::failure(
            EvolutionPluginFailure::Contract {
                violation: duplicate_issue,
            },
        ))
        .unwrap();
        assert!(decode_evolution_plugin_response(&duplicate_issue).is_err());
    }

    #[test]
    fn response_decoder_rejects_duplicates_unsafe_numbers_unknown_shapes_and_oversize() {
        let valid = serde_json::to_vec(&EvolutionPluginResponseEnvelope::failure(
            EvolutionPluginFailure::Substrate {
                code: "provider_unavailable".to_owned(),
                message: "provider is unavailable".to_owned(),
            },
        ))
        .unwrap();
        decode_evolution_plugin_response(&valid).unwrap();

        for invalid in [
            r#"{"outcome":"failure","error":{"category":"substrate","code":"provider_unavailable","message":"unavailable"}}"#.to_owned(),
            format!(
                r#"{{"outcome":"success","evolution_plugin_protocol":"{EVOLUTION_PLUGIN_PROTOCOL_VERSION}"}}"#
            ),
            r#"{"outcome":"failure","evolution_plugin_protocol":"cymule.evolution-plugin/2","error":{"category":"substrate","code":"provider_unavailable","message":"unavailable"}}"#.to_owned(),
            format!(
                r#"{{"outcome":"failure","evolution_plugin_protocol":"{EVOLUTION_PLUGIN_PROTOCOL_VERSION}","error":{{"category":"substrate","code":"provider_unavailable","code":"provider_unavailable","message":"unavailable"}}}}"#
            ),
            format!(
                r#"{{"outcome":"failure","evolution_plugin_protocol":"{EVOLUTION_PLUGIN_PROTOCOL_VERSION}","error":{{"category":"substrate","code":"provider_unavailable","message":"unavailable","unknown":true}}}}"#
            ),
            format!(
                r#"{{"outcome":"success","evolution_plugin_protocol":"{EVOLUTION_PLUGIN_PROTOCOL_VERSION}","response":{{"type":"migrated","output":{{"continuation":{{"epoch":9007199254740992}}}}}}}}"#
            ),
        ] {
            assert!(
                decode_evolution_plugin_response(invalid.as_bytes()).is_err(),
                "malformed response was admitted: {invalid}"
            );
        }

        let mut exact = valid;
        exact.resize(MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES, b' ');
        decode_evolution_plugin_response(&exact).expect("exact raw response bound is accepted");
        exact.push(b' ');
        assert!(decode_evolution_plugin_response(&exact).is_err());
    }

    #[test]
    fn closed_decoder_rejects_fractional_decimal_collisions() {
        let input = br#"{"value":0.100000000000000005}"#;
        let error = decode_closed_message::<serde_json::Value>(input)
            .expect_err("fraction collision must fail the shared evolution wire decoder");
        assert!(matches!(
            error,
            EvolutionError::Validation(message) if message.ends_with("/value")
        ));
    }

    #[test]
    fn maximum_legal_provider_products_fit_the_fixed_process_envelope() {
        let migration_output = MigrationOutput {
            continuation: dense_bounded_continuation(),
            artifacts: Vec::new(),
            evidence: maximum_migration_evidence(),
        };
        migration_output
            .verify_artifact_limits()
            .expect("maximum migration Artifact product is admitted");
        let migration = serde_json::to_vec(&EvolutionPluginResponseEnvelope::success(
            EvolutionPluginResponse::Migrated {
                output: Box::new(migration_output),
            },
        ))
        .unwrap();
        assert!(migration.len() <= MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES);
        decode_evolution_plugin_response(&migration)
            .expect("maximum legal migration response crosses the fixed process wire");

        let shadow_output = ShadowOutput {
            primary_digest: "6".repeat(64),
            shadow_digest: "7".repeat(64),
            equivalent: false,
            evidence: maximum_shadow_evidence(),
        };
        shadow_output
            .verify_evidence_limits()
            .expect("maximum shadow evidence is admitted");
        let shadow = serde_json::to_vec(&EvolutionPluginResponseEnvelope::success(
            EvolutionPluginResponse::ShadowExecuted {
                output: shadow_output,
            },
        ))
        .unwrap();
        assert!(shadow.len() <= MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES);
        decode_evolution_plugin_response(&shadow)
            .expect("maximum legal shadow response crosses the fixed process wire");
    }

    #[test]
    fn provider_product_bounds_reject_the_next_semantic_byte() {
        let mut migration_evidence = maximum_migration_evidence().bytes;
        migration_evidence.push(0);
        let migration_output = MigrationOutput {
            continuation: dense_bounded_continuation(),
            artifacts: Vec::new(),
            evidence: ArtifactRecord {
                reference: artifact_ref("cymule.test-migration-evidence/1", &migration_evidence)
                    .unwrap(),
                bytes: migration_evidence,
            },
        };
        let migration = serde_json::to_vec(&EvolutionPluginResponseEnvelope::success(
            EvolutionPluginResponse::Migrated {
                output: Box::new(migration_output),
            },
        ))
        .unwrap();
        assert!(migration.len() <= MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES);
        assert!(matches!(
            decode_evolution_plugin_response(&migration),
            Err(EvolutionError::PluginDefect { code, .. })
                if code == MigrationOutput::INVALID_ARTIFACT_PRODUCT_DEFECT_CODE
        ));

        let mut shadow_evidence = maximum_shadow_evidence().bytes;
        shadow_evidence.push(0);
        let shadow = serde_json::to_vec(&EvolutionPluginResponseEnvelope::success(
            EvolutionPluginResponse::ShadowExecuted {
                output: ShadowOutput {
                    primary_digest: "6".repeat(64),
                    shadow_digest: "7".repeat(64),
                    equivalent: false,
                    evidence: ArtifactRecord {
                        reference: artifact_ref("cymule.test-shadow-evidence/1", &shadow_evidence)
                            .unwrap(),
                        bytes: shadow_evidence,
                    },
                },
            },
        ))
        .unwrap();
        assert!(shadow.len() <= MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES);
        assert!(matches!(
            decode_evolution_plugin_response(&shadow),
            Err(EvolutionError::PluginDefect { code, .. })
                if code == ShadowOutput::INVALID_OUTPUT_DEFECT_CODE
        ));
    }
}
