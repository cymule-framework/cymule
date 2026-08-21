use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::RuntimeError;

/// Current Engine transport protocol.
pub const ENGINE_PROTOCOL_VERSION: &str = "cymule.engine/2";

/// Official directory-store provider identity understood by the CLI Engine.
pub const ENGINE_DIRECTORY_STORE_PROVIDER: &str = "cymule.directory-store/2";
/// Official SQLite-store provider identity understood by the CLI Engine.
pub const ENGINE_SQLITE_STORE_PROVIDER: &str = "cymule.sqlite-store/2";
/// Official sealed process-executor provider identity understood by the CLI Engine.
pub const ENGINE_PROCESS_EXECUTOR_PROVIDER: &str = "cymule.executor-process/1";
/// Sealed process protocol used by migration and shadow plugins.
pub const EVOLUTION_PLUGIN_PROTOCOL_VERSION: &str = "cymule.evolution-plugin/1";

/// Provider-neutral locator for one durable store instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineStoreTarget {
    /// Stable provider contract identity.
    pub provider: String,
    /// Provider-owned opaque location.
    pub location: String,
    /// Provider-owned domain when one physical store contains many domains.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
}

impl EngineStoreTarget {
    /// Address one official directory-backed domain.
    pub fn directory(location: impl Into<String>) -> Self {
        Self {
            provider: ENGINE_DIRECTORY_STORE_PROVIDER.to_owned(),
            location: location.into(),
            domain: None,
        }
    }

    /// Address one official SQLite-backed domain.
    pub fn sqlite(location: impl Into<String>, domain: impl Into<String>) -> Self {
        Self {
            provider: ENGINE_SQLITE_STORE_PROVIDER.to_owned(),
            location: location.into(),
            domain: Some(domain.into()),
        }
    }

    /// Validate the transport-level locator independently of a provider.
    pub fn verify(&self) -> EngineResult<()> {
        verify_non_empty_bound(&self.provider, 256, "store provider")?;
        verify_non_empty_bound(&self.location, 4096, "store location")?;
        if let Some(domain) = &self.domain {
            verify_non_empty_bound(domain, 512, "store domain")?;
        }
        Ok(())
    }
}

/// Provider-neutral locator for one immutable execution implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnginePluginTarget {
    /// Stable provider contract identity.
    pub provider: String,
    /// Provider-owned opaque location.
    pub location: String,
    /// Expected immutable implementation revision when exact bytes must be pinned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

impl EnginePluginTarget {
    /// Select the official sealed process executor.
    pub fn process(location: impl Into<String>) -> Self {
        Self {
            provider: ENGINE_PROCESS_EXECUTOR_PROVIDER.to_owned(),
            location: location.into(),
            revision: None,
        }
    }

    /// Select exact process bytes by their SHA-256 revision.
    pub fn pinned_process(location: impl Into<String>, revision: impl Into<String>) -> Self {
        Self {
            provider: ENGINE_PROCESS_EXECUTOR_PROVIDER.to_owned(),
            location: location.into(),
            revision: Some(revision.into()),
        }
    }

    /// Validate the transport-level locator independently of a provider.
    pub fn verify(&self) -> EngineResult<()> {
        verify_non_empty_bound(&self.provider, 256, "plugin provider")?;
        verify_non_empty_bound(&self.location, 4096, "plugin location")?;
        if let Some(revision) = &self.revision {
            if !is_sha256_id(revision) {
                return Err(EngineFailure::transport(
                    "invalid_plugin_revision",
                    "plugin revision must be a lowercase sha256 identity",
                ));
            }
        }
        Ok(())
    }
}

/// Complete provider selection for one durable Engine request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineDurableTarget {
    /// Durable state provider.
    pub store: EngineStoreTarget,
    /// Execution provider. Read-only queries intentionally omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<EnginePluginTarget>,
}

impl EngineDurableTarget {
    /// Construct a read-only target.
    pub fn query(store: EngineStoreTarget) -> Self {
        Self {
            store,
            executor: None,
        }
    }

    /// Construct a mutation target.
    pub fn execute(store: EngineStoreTarget, executor: EnginePluginTarget) -> Self {
        Self {
            store,
            executor: Some(executor),
        }
    }

    /// Validate all provider-neutral locators.
    pub fn verify(&self) -> EngineResult<()> {
        self.store.verify()?;
        if let Some(executor) = &self.executor {
            executor.verify()?;
        }
        Ok(())
    }
}

/// Complete provider selection for one live-evolution Engine request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineEvolutionTarget {
    /// Durable state provider shared with Run execution.
    pub store: EngineStoreTarget,
    /// Exact migration adapter, required only by migration commands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration: Option<EnginePluginTarget>,
    /// Exact shadow driver, required only by shadow commands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow: Option<EnginePluginTarget>,
}

impl EngineEvolutionTarget {
    /// Construct an evolution target without optional execution plugins.
    pub fn new(store: EngineStoreTarget) -> Self {
        Self {
            store,
            migration: None,
            shadow: None,
        }
    }

    /// Validate every configured locator and require exact plugin revisions.
    pub fn verify(&self) -> EngineResult<()> {
        self.store.verify()?;
        for plugin in [&self.migration, &self.shadow].into_iter().flatten() {
            plugin.verify()?;
            if plugin.revision.is_none() {
                return Err(EngineFailure::transport(
                    "unpinned_evolution_plugin",
                    "migration and shadow plugins require an exact implementation revision",
                ));
            }
        }
        Ok(())
    }
}

/// One versioned Engine request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineRequestEnvelope<T> {
    /// Frozen transport protocol version.
    pub engine_protocol: String,
    /// Closed operation payload.
    pub request: T,
}

impl<T> EngineRequestEnvelope<T> {
    /// Wrap an operation in the current Engine protocol.
    pub fn new(request: T) -> Self {
        Self {
            engine_protocol: ENGINE_PROTOCOL_VERSION.to_owned(),
            request,
        }
    }
}

/// One versioned Engine response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum EngineResponseEnvelope<T> {
    /// The operation completed and returned its typed value.
    Success {
        /// Frozen transport protocol version.
        engine_protocol: String,
        /// Closed success payload.
        response: T,
    },
    /// The operation reached a classified terminal failure.
    Failure {
        /// Frozen transport protocol version.
        engine_protocol: String,
        /// Structured failure.
        error: EngineFailure,
    },
}

impl<T> EngineResponseEnvelope<T> {
    /// Construct a successful response in the current protocol.
    pub fn success(response: T) -> Self {
        Self::Success {
            engine_protocol: ENGINE_PROTOCOL_VERSION.to_owned(),
            response,
        }
    }

    /// Construct a failed response in the current protocol.
    pub fn failure(error: EngineFailure) -> Self {
        Self::Failure {
            engine_protocol: ENGINE_PROTOCOL_VERSION.to_owned(),
            error,
        }
    }

    /// Verify the protocol version and return the operation result.
    pub fn into_result(self) -> EngineResult<T> {
        match self {
            Self::Success {
                engine_protocol,
                response,
            } => {
                verify_protocol_version(&engine_protocol)?;
                Ok(response)
            }
            Self::Failure {
                engine_protocol,
                error,
            } => {
                verify_protocol_version(&engine_protocol)?;
                error.verify()?;
                Err(error)
            }
        }
    }
}

/// Result returned by Engine transports.
pub type EngineResult<T> = std::result::Result<T, EngineFailure>;

/// Stable, closed failure categories shared by every SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineFailureCategory {
    /// The Engine process or transport could not carry a response.
    TransportFailure,
    /// Input failed structural or semantic validation.
    Validation,
    /// A declared input, output, or schema contract was violated.
    ContractViolation,
    /// Policy, capability, authority, or transition admission failed.
    AdmissionDenied,
    /// The request conflicts with durable or immutable history.
    Conflict,
    /// A referenced object does not exist.
    NotFound,
    /// A plugin returned a declared application failure.
    ExpectedPluginFailure,
    /// A plugin violated its protocol or terminated defectively.
    PluginDefect,
    /// A concrete process, store, network, or other substrate failed.
    SubstrateFailure,
    /// The operation was explicitly cancelled.
    Cancelled,
    /// The operation exceeded its admitted deadline.
    TimedOut,
    /// The external world may have changed but no authoritative outcome exists.
    UnknownWorldOutcome,
}

/// Stable Engine processing phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnginePhase {
    /// Starting, writing to, reading from, or decoding the Engine transport.
    Transport,
    /// Decoding the versioned request envelope.
    DecodeRequest,
    /// Validating the request envelope and selected operation.
    ValidateRequest,
    /// Sealing a Plan Candidate.
    SealPlan,
    /// Verifying a Sealed Plan.
    VerifyPlan,
    /// Sealing a Resource Candidate.
    SealResource,
    /// Verifying a wait activation.
    VerifyWaitActivation,
    /// Verifying a durable command.
    VerifyDurableCommand,
    /// Verifying an evolution command.
    VerifyEvolutionCommand,
    /// Verifying a unified live-evolution command.
    VerifyLiveEvolutionCommand,
    /// Executing a Plan.
    ExecutePlan,
    /// Executing one stateful durable command.
    ExecuteDurable,
    /// Executing one stateful live-evolution command.
    ExecuteLiveEvolution,
    /// Describing a selected plugin.
    PluginDescribe,
    /// Calling a component plugin.
    PluginCall,
    /// Preparing an external effect.
    EffectPrepare,
    /// Dispatching an external effect.
    EffectDispatch,
    /// Reconciling an ambiguous external effect.
    EffectReconcile,
    /// Encoding the response envelope.
    EncodeResponse,
}

/// Side of a declared contract that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineContractSide {
    /// The schema declaration itself is invalid.
    Schema,
    /// A value supplied to an operation is invalid.
    Input,
    /// A value returned by an operation is invalid.
    Output,
}

/// A stable retry or recovery disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineRetryDisposition {
    /// Repeating the request cannot make progress without a semantic change.
    Never,
    /// Correct the request or contract, then submit a new semantic request.
    CorrectAndRetry,
    /// Refresh the causal precondition, then retry the same semantic intent.
    RefreshAndRetry,
    /// Retry the identical request and identity after substrate recovery.
    RetrySameRequest,
    /// Reconcile the original external intent before any further dispatch.
    Reconcile,
}

/// One machine-readable validation or contract issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineIssue {
    /// Stable issue code.
    pub code: Box<str>,
    /// Human-readable issue summary.
    pub message: Box<str>,
    /// JSON Pointer relative to the selected contract side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<Box<str>>,
    /// JSON Pointer to the failing schema keyword for contract issues.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_path: Option<Box<str>>,
}

/// One structured Engine failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineFailure {
    /// Stable semantic category.
    pub category: EngineFailureCategory,
    /// Processing phase in which the failure became authoritative.
    pub phase: EnginePhase,
    /// Stable implementation-independent error code.
    pub code: Box<str>,
    /// Human-readable summary that is not used for control flow.
    pub message: Box<str>,
    /// Declared contract identity, when the failure is contract-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<Box<str>>,
    /// Contract side, when the failure is contract-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_side: Option<EngineContractSide>,
    /// Primary JSON Pointer, when one location is authoritative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<Box<str>>,
    /// Complete bounded issue set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<EngineIssue>,
    /// Permitted recovery behavior, when the Engine can state it safely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_disposition: Option<EngineRetryDisposition>,
}

impl EngineFailure {
    /// Construct a failure without optional contract detail.
    pub fn new(
        category: EngineFailureCategory,
        phase: EnginePhase,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            category,
            phase,
            code: code.into().into_boxed_str(),
            message: message.into().into_boxed_str(),
            contract: None,
            contract_side: None,
            path: None,
            issues: Vec::new(),
            retry_disposition: None,
        }
    }

    /// Construct a transport failure for a client that could not obtain an envelope.
    pub fn transport(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(
            EngineFailureCategory::TransportFailure,
            EnginePhase::Transport,
            code,
            message,
        )
    }

    /// Verify all bounded wire fields after deserialization.
    pub fn verify(&self) -> EngineResult<()> {
        verify_code(&self.code, 200, "failure code")?;
        verify_non_empty_bound(&self.message, 8192, "failure message")?;
        if let Some(contract) = &self.contract {
            verify_non_empty_bound(contract, 500, "contract")?;
        }
        if let Some(path) = &self.path {
            verify_path(path, "failure path")?;
        }
        if self.issues.len() > 100 {
            return Err(invalid_failure("failure issues exceed 100 entries"));
        }
        for issue in &self.issues {
            verify_non_empty_bound(&issue.code, 200, "issue code")?;
            verify_non_empty_bound(&issue.message, 2000, "issue message")?;
            if let Some(path) = &issue.path {
                verify_path(path, "issue path")?;
            }
            if let Some(path) = &issue.schema_path {
                verify_path(path, "issue schema path")?;
            }
        }
        Ok(())
    }

    /// Project a trusted-kernel error at an exact Engine boundary.
    pub fn from_core(error: &cymule_core::CoreError, phase: EnginePhase) -> Self {
        use cymule_core::CoreError;

        let (category, retry_disposition) = match &error {
            CoreError::Validation(_) => (
                EngineFailureCategory::Validation,
                Some(EngineRetryDisposition::CorrectAndRetry),
            ),
            CoreError::NotFound(_) => (EngineFailureCategory::NotFound, None),
            CoreError::IdentityMismatch(_) | CoreError::Causal(_) | CoreError::Encoding(_) => (
                EngineFailureCategory::ContractViolation,
                Some(EngineRetryDisposition::Never),
            ),
            CoreError::IllegalTransition(_) => (
                EngineFailureCategory::AdmissionDenied,
                Some(EngineRetryDisposition::CorrectAndRetry),
            ),
            CoreError::CommandReuse(_) => (
                EngineFailureCategory::Conflict,
                Some(EngineRetryDisposition::Never),
            ),
        };
        let mut failure = Self::new(category, phase, error.code(), error.to_string());
        failure.retry_disposition = retry_disposition;
        failure
    }

    /// Project an embedded-runtime error at an exact Engine boundary.
    pub fn from_runtime(error: RuntimeError, phase: EnginePhase) -> Self {
        match error {
            RuntimeError::Core(error) => Self::from_core(&error, phase),
            RuntimeError::Contract(error) => Self::from_contract_violation(&error, phase),
            RuntimeError::ExpectedPluginFailure(error) => {
                let mut failure = Self::new(
                    EngineFailureCategory::ExpectedPluginFailure,
                    phase,
                    error.code,
                    error.message,
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::PluginDefect { code, message } => {
                let mut failure =
                    Self::new(EngineFailureCategory::PluginDefect, phase, code, message);
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::Suspended(boundary) => {
                let mut failure = Self::new(
                    EngineFailureCategory::AdmissionDenied,
                    phase,
                    "embedded_profile_suspended",
                    format!(
                        "wait site {} reached binding {:?}",
                        boundary.site_id, boundary.result_bind
                    ),
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::ReleaseRequired { intent_ids } => {
                let mut failure = Self::new(
                    EngineFailureCategory::AdmissionDenied,
                    phase,
                    "effect_release_required",
                    format!(
                        "prepared explicit effects require durable caller release: {}",
                        intent_ids.join(",")
                    ),
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::Substrate { code, message } => {
                let mut failure = Self::new(
                    EngineFailureCategory::SubstrateFailure,
                    phase,
                    code,
                    message,
                );
                failure.retry_disposition = Some(EngineRetryDisposition::RetrySameRequest);
                failure
            }
            RuntimeError::TimedOut { code, message } => {
                let mut failure = Self::new(EngineFailureCategory::TimedOut, phase, code, message);
                failure.retry_disposition = Some(EngineRetryDisposition::RetrySameRequest);
                failure
            }
            RuntimeError::UnknownWorld { code, message } => {
                let mut failure = Self::new(
                    EngineFailureCategory::UnknownWorldOutcome,
                    phase,
                    code,
                    message,
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Reconcile);
                failure
            }
            RuntimeError::Encoding(message) => {
                let mut failure = Self::new(
                    EngineFailureCategory::ContractViolation,
                    phase,
                    "runtime_encoding_failed",
                    message,
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
        }
    }

    /// Project an executable Plan contract violation without flattening its
    /// boundary, side, or issue paths.
    pub fn from_contract_violation(error: &crate::ContractViolation, phase: EnginePhase) -> Self {
        use crate::{ContractBoundary, ContractPhase, ContractSide};

        let contract_kind = match error.target.boundary {
            ContractBoundary::Definition => "definition",
            ContractBoundary::Component => "component",
            ContractBoundary::Effect => "effect",
            ContractBoundary::Wait => "wait",
        };
        let contract_side = match error.phase {
            ContractPhase::Admission => EngineContractSide::Schema,
            ContractPhase::Execution => match error.target.side {
                ContractSide::Input => EngineContractSide::Input,
                ContractSide::Output => EngineContractSide::Output,
            },
        };
        let code = match error.phase {
            ContractPhase::Admission => "invalid_contract_schema",
            ContractPhase::Execution => "contract_value_mismatch",
        };
        let mut failure = Self::new(
            EngineFailureCategory::ContractViolation,
            phase,
            code,
            format!(
                "{contract_kind} {:?} {:?} contract rejected {} issue(s)",
                error.target.id,
                contract_side,
                error.issues.len()
            ),
        );
        failure.contract = Some(format!("{contract_kind}:{}", error.target.id).into_boxed_str());
        failure.contract_side = Some(contract_side);
        failure.path = error
            .issues
            .first()
            .map(|issue| issue.instance_path.clone().into_boxed_str());
        failure.issues = error
            .issues
            .iter()
            .map(|issue| EngineIssue {
                code: code.into(),
                message: issue.message.clone().into_boxed_str(),
                path: Some(issue.instance_path.clone().into_boxed_str()),
                schema_path: Some(issue.schema_path.clone().into_boxed_str()),
            })
            .collect();
        failure.retry_disposition = Some(match error.phase {
            ContractPhase::Admission | ContractPhase::Execution => {
                EngineRetryDisposition::CorrectAndRetry
            }
        });
        failure
    }
}

impl Display for EngineFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for EngineFailure {}

fn verify_protocol_version(version: &str) -> EngineResult<()> {
    if version == ENGINE_PROTOCOL_VERSION {
        return Ok(());
    }
    let mut failure = EngineFailure::new(
        EngineFailureCategory::ContractViolation,
        EnginePhase::Transport,
        "unsupported_engine_protocol",
        format!("expected {ENGINE_PROTOCOL_VERSION}, received {version:?}"),
    );
    failure.contract = Some(ENGINE_PROTOCOL_VERSION.into());
    failure.contract_side = Some(EngineContractSide::Schema);
    failure.retry_disposition = Some(EngineRetryDisposition::Never);
    Err(failure)
}

fn is_sha256_id(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

fn verify_code(value: &str, max: usize, label: &str) -> EngineResult<()> {
    verify_non_empty_bound(value, max, label)?;
    if !value.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() && index > 0 || byte == b'_'
    }) || !value.as_bytes()[0].is_ascii_lowercase()
    {
        return Err(invalid_failure(format!(
            "{label} must match ^[a-z][a-z0-9_]*$"
        )));
    }
    Ok(())
}

fn verify_non_empty_bound(value: &str, max: usize, label: &str) -> EngineResult<()> {
    if value.is_empty() || value.len() > max {
        return Err(invalid_failure(format!(
            "{label} must contain 1 to {max} bytes"
        )));
    }
    Ok(())
}

fn verify_path(path: &str, label: &str) -> EngineResult<()> {
    if path.len() > 1000 || !path.is_empty() && !path.starts_with('/') {
        return Err(invalid_failure(format!(
            "{label} must be an empty or slash-prefixed JSON Pointer of at most 1000 bytes"
        )));
    }
    Ok(())
}

fn invalid_failure(message: impl Into<String>) -> EngineFailure {
    EngineFailure::new(
        EngineFailureCategory::ContractViolation,
        EnginePhase::Transport,
        "invalid_engine_failure",
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_failure_does_not_claim_retry_is_safe() {
        assert_eq!(
            EngineFailure::transport("engine_failed", "no envelope").retry_disposition,
            None
        );
    }

    #[test]
    fn failure_envelope_rejects_unknown_fields_and_invalid_bounds() {
        let unknown = serde_json::json!({
            "outcome": "failure",
            "engine_protocol": ENGINE_PROTOCOL_VERSION,
            "error": {
                "category": "validation",
                "phase": "seal_plan",
                "code": "validation_failed",
                "message": "invalid",
                "provider": "must-not-enter-engine-errors"
            }
        });
        assert!(
            serde_json::from_value::<EngineResponseEnvelope<serde_json::Value>>(unknown).is_err()
        );

        let invalid = EngineResponseEnvelope::<serde_json::Value>::failure(EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::SealPlan,
            "INVALID-CODE",
            "invalid",
        ));
        let failure = invalid
            .into_result()
            .expect_err("invalid failure code is rejected after deserialization");
        assert_eq!(failure.code.as_ref(), "invalid_engine_failure");
    }
}
